#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

set(AVRO_SRC
    allocation.c
    array.c
    avro.h
    avro/allocation.h
    avro/basics.h
    avro/consumer.h
    avro/data.h
    avro/errors.h
    avro/generic.h
    avro/io.h
    avro/legacy.h
    avro/refcount.h
    avro/resolver.h
    avro/schema.h
    avro/value.h
    avro_generic_internal.h
    avro_private.h
    codec.c
    codec.h
    consumer.c
    consume-binary.c
    datafile.c
    datum.c
    datum.h
    datum_equal.c
    datum_read.c
    datum_size.c
    datum_skip.c
    datum_validate.c
    datum_value.c
    datum_write.c
    dump.c
    dump.h
    encoding.h
    encoding_binary.c
    errors.c
    generic.c
    io.c
    map.c
    memoize.c
    resolved-reader.c
    resolved-writer.c
    resolver.c
    schema.c
    schema.h
    schema_equal.c
    st.c
    st.h
    string.c
    value.c
    value-hash.c
    value-json.c
    value-read.c
    value-sizeof.c
    value-write.c
    wrapped-buffer.c
)


source_group(Avro FILES ${AVRO_SRC})

# The version.sh script gives us a SOVERSION that uses colon as a
# separator; we need periods.

string(REPLACE ":" "." LIBAVRO_DOT_VERSION ${LIBAVRO_VERSION})

add_library(avro-static STATIC ${AVRO_SRC})
target_link_libraries(avro-static ${JANSSON_LIBRARIES} ${CODEC_LIBRARIES} ${THREADS_LIBRARIES})
set_target_properties(avro-static PROPERTIES OUTPUT_NAME avro)
