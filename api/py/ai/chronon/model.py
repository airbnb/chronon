#     Copyright (C) 2025 The Chronon Authors.
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

import logging
from typing import Dict, List, Optional, Tuple

import ai.chronon.api.ttypes as api
from ai.chronon.join import DataType

logging.basicConfig(level=logging.INFO)

FieldsType = List[Tuple[str, api.TDataType]]


def InferenceSpec(
    model_backend: str,
    model_backend_params: Dict[str, str],
) -> api.InferenceSpec:
    return api.InferenceSpec(
        modelBackend=model_backend,
        modelBackendParams=model_backend_params,
    )


def Model(
    name: str, inference_spec: api.InferenceSpec, input_schema: FieldsType, output_schema: FieldsType
) -> api.Model:
    metadata = api.MetaData(name=name)
    return api.Model(
        metaData=metadata,
        inferenceSpec=inference_spec,
        inputSchema=DataType.STRUCT(f"model_{name}_inputs", *input_schema),
        outputSchema=DataType.STRUCT(f"model_{name}_outputs", *output_schema),
    )


def ModelTransform(
    model: api.Model,
    input_mappings: Optional[Dict[str, str]] = None,
    output_mappings: Optional[Dict[str, str]] = None,
    prefix: Optional[str] = None,
) -> api.ModelTransform:
    return api.ModelTransform(
        model=model,
        inputMappings=input_mappings,
        outputMappings=output_mappings,
        prefix=prefix,
    )


def ModelTransforms(
    transforms: List[api.ModelTransform],
    passthrough_fields: Optional[List[str]] = None,
) -> api.ModelTransforms:
    return api.ModelTransforms(
        transforms=transforms,
        passthroughFields=passthrough_fields,
    )
