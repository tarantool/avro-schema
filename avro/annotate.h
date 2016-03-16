void *avro_schema_record_annotation(const avro_schema_t record)
	__attribute__((__pure__));

struct annotation
{
	size_t full_size;
	size_t full_bitmap_size;
	size_t offsets[0];
};

struct annotation *create_annotation(size_t num_fields)
{
	annotation *a =
		(annotation *)malloc(
			offsetof(struct annotation, offsets) +
			2 * num_fields * sizeof(a->offsets[0]));
	if (!a)
		throw std::bad_alloc();
	a->full_size = 0;
	return a;
}

size_t *get_flattened_item_offsets(annotation *a, size_t num_fields)
{
	(void)num_fields;
	return a->offsets;
}

size_t *get_nested_bitmap_offsets(annotation *a, size_t num_fields)
{
	return a->offsets + num_fields;
}

void annotate(avro_schema_t schema)
{
	switch (avro_typeof(schema)) {

	case AVRO_RECORD:
	{
		size_t n = avro_schema_record_size(schema);
		annotation *a = create_annotation(n);

		avro_schema_record_annotation_set(schema, a);

		size_t *item_offsets =
			get_flattened_item_offsets(a, n);

		size_t *nbitmap_offsets =
			get_nested_bitmap_offsets(a, n);

		size_t next_item_offset = 0;
		size_t next_nbitmap_offset = n;

		for (size_t i = 0; i < n; i++) {

			avro_schema_t item =
				avro_schema_record_field_get_by_index(
					schema, i);

			annotate(item);

			item_offsets[i] = next_item_offset;
			nbitmap_offsets[i] = next_nbitmap_offset;

rematch:
			switch (avro_typeof(item)) {
			case AVRO_LINK:
				item = avro_schema_link_target(
					item);
				goto rematch;
			case AVRO_RECORD:
				{
					annotation *a2 = (annotation *)
						avro_schema_record_annotation(
							item);

					assert(a2);
					assert(a2->full_size != 0);

					next_item_offset +=
						a2->full_size;

					next_nbitmap_offset +=
						a2->full_bitmap_size;
				}
				break;
			case AVRO_UNION:
				next_item_offset += 2;
				break;
			default:
				next_item_offset ++;
				break;
			}
		}
		a->full_size = next_item_offset;
		a->full_bitmap_size = next_nbitmap_offset;
		return;
	}
	case AVRO_MAP:
		return annotate(avro_schema_map_values(schema));
	case AVRO_ARRAY:
		return annotate(avro_schema_array_items(schema));
	case AVRO_UNION:
	{
		size_t n = avro_schema_union_size(schema);
		for (size_t i = 0; i < n; i++ ) {
			annotate(avro_schema_union_branch(
					schema, i));
		}
		return;
	}
	default:
		return;
	}
}
