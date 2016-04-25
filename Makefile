avro_schema_rt_c.so: avro_schema_rt.c
	gcc -O3 -DNDEBUG -std=c99 -fno-strict-aliasing -fPIC -shared avro_schema_rt.c -o avro_schema_rt_c.so

