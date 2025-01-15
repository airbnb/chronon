
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

from ai.chronon.join import Join, JoinPart
from group_bys.kaggle.outbrain import ad_doc, ad_uuid, ad_platform
from sources.kaggle.outbrain import outbrain_left_events
from group_bys.kaggle.clicks import ad_streaming

training_set = Join(  # left equi join
    left=outbrain_left_events(
        "uuid", "display_id", "ad_id", "document_id", "clicked", "geo_location", "platform"),
    right_parts=[JoinPart(group_by=group_by) for group_by in [ad_doc, ad_uuid, ad_streaming, ad_platform]]
)
