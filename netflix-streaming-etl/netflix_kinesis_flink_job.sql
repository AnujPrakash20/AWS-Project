
-- Create source table for Kinesis stream

CREATE TABLE netflix_video_stream (
    chunk_id INT,
    bucket STRING,
    timestamp STRING,
    bitrate STRING,
    file_size STRING,
    last_modified STRING,
    video_id STRING,
    processing_pipeline STRING,
    proc_time AS PROCTIME()
) WITH (
    'connector' = 'kinesis',
    'stream' = 'netflix_video_stream',
    'aws.region' = 'us-east-1',
    'aws.credentials.provider' = 'BASIC',
    'aws.credentials.basic.accesskeyid' = 'Your AWS Access Key ID',
    'aws.credentials.basic.secretkey' = 'Your AWS Secret Access Key',
    'format' = 'json',
    'scan.stream.initpos' = 'TRIM_HORIZON'
);

-- Create output table

CREATE TABLE processed_video_analytics (
    video_id STRING,
    total_chunks INT,
    processing_time STRING,
    avg_file_size STRING,
    PRIMARY KEY (video_id) NOT ENFORCED
) WITH (
    'connector' = 'print'
);

-- Process and insert data
INSERT INTO processed_video_analytics
SELECT 
    video_id,
    COUNT(*) as total_chunks,
    CAST(proc_time AS STRING) as processing_time,
    file_size as avg_file_size
FROM netflix_video_stream
GROUP BY video_id, file_size, proc_time;
