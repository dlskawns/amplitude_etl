CREATE TABLE ctas_json_partitioned WITH (
     format = 'JSON', 
     external_location = 's3://amplitude-bucket-mf/anal_practice/', 
     partitioned_by = ARRAY['month']) 
AS SELECT "user_id",
     "event_type",
     "event_id",
     "city",
     json_extract_scalar(event_properties, '$.referrer') referrer,
     json_extract_scalar(event_properties, '$.service_name') service_name,
     json_extract_scalar(event_properties, '$.input1') input1,
     json_extract_scalar(event_properties, '$.input2') input2,
     json_extract_scalar(event_properties, '$.input3') input3,
     json_extract_scalar(event_properties, '$.recommend_id') recommend_id,
     json_extract_scalar(event_properties, '$.status') status,
     month(event_time) "month"
FROM amplitude_data.event_stream;