"""Object for checking whether a Chronon API thrift object is consistent with other
"""
import json
import logging
import os
from ai.chronon.api.ttypes import \
    GroupBy, Join, Source
from ai.chronon.logger import get_logger
from ai.chronon.repo import JOIN_FOLDER_NAME, \
    GROUP_BY_FOLDER_NAME
from ai.chronon.repo.serializer import \
    thrift_simple_json, file2thrift
from collections import defaultdict
from typing import List, Dict

# Fields that indicate stutus of the entities.
SKIPPED_FIELDS = frozenset(['metaData'])
EXTERNAL_KEY = 'onlineExternalParts'


def _filter_skipped_fields_from_join(json_obj: Dict, skipped_fields):
    for join_part in json_obj['joinParts']:
        group_by = join_part['groupBy']
        for field in skipped_fields:
            group_by.pop(field, None)
    if EXTERNAL_KEY in json_obj:
        json_obj.pop(EXTERNAL_KEY, None)


def is_valid_conf(conf: object) -> bool:
    return conf.metaData is not None


def extract_json_confs(obj_class: type, path: str) -> List[object]:
    if os.path.isfile(path):
        conf = file2thrift(path, obj_class)
        return [conf] if is_valid_conf(conf) else []
    result = []
    for sub_root, sub_dirs, sub_files in os.walk(path):
        for f in sub_files:
            if not f.startswith('.'):  # ignore hidden files - such as .DS_Store
                obj = file2thrift(os.path.join(sub_root, f), obj_class)
                if is_valid_conf(obj):
                    result.append(obj)
    return result


def is_batch_upload_needed(group_by: GroupBy) -> bool:
    if group_by.metaData.online or group_by.backfillStartDate:
        return True
    else:
        return False


class ChrononRepoValidator(object):
    def __init__(self, chronon_root_path: str, output_root: str, log_level=logging.INFO):
        self.logger = get_logger(log_level)
        self.old_objs = defaultdict(dict)
        # returned key has "group_by." prefix in the name so we remove the prefix.
        self.chronon_root_path = chronon_root_path
        self.output_root = output_root
        self.log_level = log_level
        self.logger = get_logger(log_level)
        self.load_objs()

    def load_objs(self):
        # we keep the objs in the list not in  a set since thrift does not
        # implement __hash__ for ttypes object.
        self.old_group_bys = extract_json_confs(
            GroupBy,
            os.path.join(self.chronon_root_path, self.output_root, GROUP_BY_FOLDER_NAME))
        self.old_joins = extract_json_confs(
            Join,
            os.path.join(self.chronon_root_path, self.output_root, JOIN_FOLDER_NAME))

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
            if not is_batch_upload_needed(obj):
                reasons.append("GroupBys should not be materialized if batch upload job is not needed")
            # Otherwise group_bys included in online join or are marked explicitly
            # online itself are materialized.
            elif not any(join.metaData.online for join in self._get_old_joins_with_group_by(obj)) \
                    and not is_batch_upload_needed(obj):
                reasons.append("is not marked online/production nor is included in any online join")
        return reasons

    def validate_obj(self, obj: object) -> List[str]:
        """
        Validate Chronon API obj against other entities in the repo.

        returns:
          list of errors.
        """
        if isinstance(obj, GroupBy):
            return self._validate_group_by(obj)
        elif isinstance(obj, Join):
            return self._validate_join(obj)
        return []

    def _has_diff(
            self,
            obj: object,
            old_obj: object,
            skipped_fields=SKIPPED_FIELDS) -> bool:
        new_json = {k: v for k, v in json.loads(thrift_simple_json(obj)).items()
                    if k not in skipped_fields}
        old_json = {k: v for k, v in json.loads(thrift_simple_json(old_obj)).items()
                    if k not in skipped_fields}
        if isinstance(obj, Join):
            _filter_skipped_fields_from_join(new_json, skipped_fields)
            _filter_skipped_fields_from_join(old_json, skipped_fields)
        return new_json != old_json

    def safe_to_overwrite(self, obj: object) -> bool:
        """When an object is already materialized as online, it is no more safe
        to materialize and overwrite the old conf.
        """
        old_obj = self._get_old_obj(type(obj), obj.metaData.name)
        return not old_obj or not self._has_diff(obj, old_obj) or not old_obj.metaData.online

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
            errors.append("join {} is production but includes the following non production group_bys: {}".format(
                join.metaData.name, ', '.join(non_prod_old_group_bys)))
        if join.metaData.online:
            if offline_included_group_bys:
                errors.append("join {} is online but includes the following offline group_bys: {}".format(
                    join.metaData.name, ', '.join(offline_included_group_bys)))
            # If join is online we materialize the underlying group bys
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
        if prod_joins:
            if group_by.metaData.production is False:
                errors.append(
                    "group_by {} is explicitly marked as non-production but included in the following production "
                    "joins: {}".format(
                        group_by.metaData.name, ', '.join(prod_joins)))
            # if the group by is included in any of materialized production join,
            # set it to production in the materialized output.
            else:
                group_by.metaData.production = True

        for source in group_by.sources:
            src: Source = source
            if src.events and src.events.isCumulative and (src.events.query.timeColumn is None):
                errors.append(
                    "Please set query.timeColumn for Cumulative Events Table: {}".format(src.events.table))
        return errors
