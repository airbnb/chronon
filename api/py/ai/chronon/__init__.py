
#     Copyright (C) 2023 The Chronon Authors.
#
#     Licensed under the Apache License, Version 2.0 (the "License");
#     you may not use this file except in compliance with the License.
#     You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#     Unless required by applicable law or agreed to in writing, software
#     distributed under the License is distributed on an "AS IS" BASIS,
#     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#     See the License for the specific language governing permissions and
#     limitations under the License.

import ai.chronon.api.ttypes as ttypes
import inspect
import json


# Takes in an conf object class like GroupBy, Join and StagingQuery
# And returns a function that dispatches the arguments correctly to the object class and inner metadata
# Remaining args will end up in object.metaData.customJson
def _metadata_shim(conf_class):
    constructor_params = list(inspect.signature(conf_class.__init__).parameters.keys())
    assert constructor_params[0] == "self", "First param should be 'self', found {}".format(
        constructor_params[0])
    assert constructor_params[1] == "metaData", "Second param should be 'metaData', found {}".format(
        constructor_params[1])
    outer_params = constructor_params[2:]
    metadata_params = list(inspect.signature(ttypes.MetaData.__init__).parameters.keys())[1:]
    intersected_params = set(outer_params) & set(metadata_params)
    unioned_params = set(outer_params) | set(metadata_params)
    err_msg = "Cannot shim {}, because params: {} are intersecting with MetaData's params".format(
        conf_class, intersected_params)
    assert len(intersected_params) == 0, err_msg

    def shimmed_func(**kwargs):
        meta_kwargs = {key: value for key, value in kwargs.items() if key in metadata_params}
        outer_kwargs = {key: value for key, value in kwargs.items() if key in outer_params}
        custom_json_args = {key: value for key, value in kwargs.items() if key not in unioned_params}
        meta = ttypes.MetaData(customJson=json.dumps(custom_json_args), **meta_kwargs)
        return conf_class(metaData=meta, **outer_kwargs)
    return shimmed_func


StagingQuery = _metadata_shim(ttypes.StagingQuery)
