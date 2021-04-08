"""Object for checking whether a Zipline API thrift object is consistent with other
"""
import ai.zipline.utils as utils
import json
import logging
import os
import shutil
import subprocess
import tempfile
from ai.zipline.api.ttypes import \
    GroupBy, Join
from ai.zipline.logger import get_logger
from ai.zipline.repo import JOIN_FOLDER_NAME, \
    GROUP_BY_FOLDER_NAME
from ai.zipline.repo.serializer import \
    thrift_simple_json, file2thrift
from collections import defaultdict
from typing import List, Optional

# Fields that indicate stutus of the entities.
STATUS_FIELDS = frozenset(['online', 'production'])


def get_input(text):
    return input(text)


def extract_json_confs(obj_class: type, path: str):
    if os.path.isfile(path):
        return [file2thrift(path, obj_class)]
    result = []
    for sub_root, sub_dirs, sub_files in os.walk(path):
        for f in sub_files:
            obj = file2thrift(os.path.join(sub_root, f), obj_class)
            result.append(obj)
    return result


class ZiplineRepoValidator(object):
    def __init__(self, zipline_root_path: str, output_root: str, log_level=logging.INFO):
        self.logger = get_logger(log_level)
        self.old_objs = defaultdict(dict)
        # returned key has "group_by." prefix in the name so we remove the prefix.
        self.zipline_root_path = zipline_root_path
        self.output_root = output_root
        self.log_level = log_level
        self.logger = get_logger(log_level)
        self.load_objs()

    def load_objs(self):
        # we keep the objs in the list not in  a set since thrift does not
        # implement __hash__ for ttypes object.
        self.old_group_bys = extract_json_confs(
            GroupBy,
            os.path.join(self.zipline_root_path, self.output_root, GROUP_BY_FOLDER_NAME))
        self.old_joins = extract_json_confs(
            Join,
            os.path.join(self.zipline_root_path, self.output_root, JOIN_FOLDER_NAME))

        self.old_objs['GroupBy'] = self.old_group_bys
        self.old_objs['Join'] = self.old_joins

    def _get_old_obj(self, obj_class: type, obj_name: str) -> object:
        """
        returns:
           materialized version of the obj given the object's name.
        """
        return next(
            (x for x in self.old_objs[obj_class.__name__] if x.metaData and x.metaData.name == obj_name),
            None
        )

    def _get_old_joins_with_group_by(self, group_by: GroupBy) -> List[Join]:
        """
        returns:
            materialized joins including the group_by as dicts.
        """
        return [join for join in self.old_joins if join.joinParts is not None and
                group_by.metaData.name in [rp.groupBy.metaData.name for rp in join.joinParts]]

    def can_skip_materialize(self, obj: object) -> List[str]:
        """
        Check if the object can be skipped to be materialized and return reasons
        if it can be.
        """
        reasons = []
        if isinstance(obj, GroupBy):
            # GroupBys explicitly marked as offline should not be materialized.
            if obj.metaData.online is False and obj.metaData.production is False:
                reasons.append("is explicitly marked as offline")
            # Otherwise group_bys included in online join or are marked explicitly
            # online itself are materialized.
            elif not any(join.metaData.online for join in self._get_old_joins_with_group_by(obj)) \
                    and not obj.metaData.online and not obj.metaData.production:
                reasons.append("is not marked online/production nor is included in any online join")
        return reasons

    def _safe_to_overwrite(self, obj: object) -> bool:
        """
        When a feature set is already materialized as online, it is no more safe
        to materialize without user permission.
        """
        return obj.metaData.online is not True

    def validate_obj(self, obj: object) -> List[str]:
        """
        Validate Zipline API obj against other entities in the repo.

        returns:
          list of errors.
        """
        if isinstance(obj, GroupBy):
            return self._validate_group_by(obj)
        elif isinstance(obj, Join):
            return self._validate_join(obj)
        return []

    def approve_diff(self, obj: object) -> bool:
        """If the obj is already materialized, check if it is safe to overwrite
        the old conf.
        """
        obj_class = type(obj).__name__
        old_obj = self._get_old_obj(type(obj), obj.metaData.name)
        if old_obj and not self._safe_to_overwrite(old_obj):
            try:
                differ = utils.JsonDiffer()
                diff = differ.diff(thrift_simple_json(obj), thrift_simple_json(old_obj))
                if diff:
                    prompt = f"""
You are trying to overwrite a protected {obj_class} conf. See the diff:
(first line above --- is from the new conf, second line is from the old conf)

{diff}

You can see the files here:

old conf - {differ.old_name}
new conf = {differ.new_name}

Replace old {obj.metaData.name} with new configuration? If so, type y or yes.\n
"""
                    ans = get_input(prompt)
                    return ans.strip().lower() in ["y", "yes"]
            finally:
                differ.clean()
        return True

    def _get_diff(
            self,
            obj: object,
            old_obj: object,
            skipped_keys=STATUS_FIELDS,
            to_dir: Optional[str] = None,
            new_name: str = 'new.json',
            old_name: str = 'old.json') -> str:
        new_json = {k: v for k, v in json.loads(thrift_simple_json(obj)).items()
                    if k not in skipped_keys}
        old_json = {k: v for k, v in json.loads(thrift_simple_json(old_obj)).items()
                    if k not in skipped_keys}
        try:
            if to_dir:
                temp_dir = to_dir
            else:
                temp_dir = tempfile.mkdtemp()
            with open(os.path.join(temp_dir, old_name), mode='w') as old, \
                    open(os.path.join(temp_dir, new_name), mode='w') as new:
                old.write(json.dumps(old_json, sort_keys=True, indent=2))
                new.write(json.dumps(new_json, sort_keys=True, indent=2))

            return subprocess.run(
                ['diff', old.name, new.name], stdout=subprocess.PIPE).stdout.decode('utf-8')
        finally:
            if not to_dir:
                shutil.rmtree(temp_dir)

    def _validate_join(self, join: Join) -> List[str]:
        """
        Validate join's status with materialized versions of group_bys
        included by the join.

        Returns:
          list of validation errors.
        """
        included_group_bys = [rp.groupBy for rp in join.joinParts]
        offline_included_group_bys = [gb.metaData.name for gb in included_group_bys
                                      if not gb.metaData or gb.metaData.online is False]
        errors = []
        old_group_bys = [group_by for group_by in included_group_bys
                         if self._get_old_obj(GroupBy, group_by.metaData.name)]
        non_prod_old_group_bys = [group_by.metaData.name for group_by in old_group_bys
                                  if group_by.metaData.production is False]
        if join.metaData.production and non_prod_old_group_bys:
            errors.append("join {} is production but includes "
                          "the following non production group_bys: {}".format(
                              join.metaData.name, ', '.join(non_prod_old_group_bys)))
        if join.metaData.online:
            if offline_included_group_bys:
                errors.append("join {} is online but includes "
                              "the following offline group_bys: {}".format(
                                  join.metaData.name, ', '.join(offline_included_group_bys)))
            # If join is online we materialize the underlying group bys are materialized
            # So we need to check if they are valid.
            group_by_errors = [self._validate_group_by(group_by) for group_by in included_group_bys]
            errors += [f"join {join.metaData.name}'s underlying {error}"
                       for errors in group_by_errors for error in errors]
        return errors

    def _validate_group_by(self, group_by: GroupBy) -> List[str]:
        """
        Validate group_by's status with materialized versions of joins
        including the group_by.

        Return:
          List of validation errors.
        """
        joins = self._get_old_joins_with_group_by(group_by)
        online_joins = [join.metaData.name for join in joins if join.metaData.online is True]
        prod_joins = [join.metaData.name for join in joins if join.metaData.production is True]
        errors = []
        # group by that are marked explicitly offline should not be present in
        # materialized online joins.
        if group_by.metaData.online is False and online_joins:
            errors.append(
                "group_by {} is explicitly marked offline but included in "
                "the following online joins: {}".format(
                    group_by.metaData.name, ", ".join(online_joins)))
        # group by that are marked explicitly non-production should not be
        # present in materialized production joins.
        self.logger.debug(prod_joins)
        self.logger.debug("new")
        self.logger.debug(self.old_joins)
        if prod_joins:
            if group_by.metaData.production is False:
                errors.append("group_by {} is explicitly marked as non-production "
                              "but included in the following production joins: {}".format(
                                  group_by.metaData.name, ', '.join(prod_joins)))
            # if the group by is included in any of materialized production join,
            # set it to production in the materialized output.
            else:
                group_by.metaData.production = True
        return errors
