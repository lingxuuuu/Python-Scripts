import subprocess
path = "s3a://gd-de-dp-pr-hcat-engagement-schema/backfill_events/event_jobsearch/dev-ds=1970-01-01"
args = "hdfs dfs -ls "+path+" | awk '{print $8}'"
proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
s_output, s_err = proc.communicate()
dirs = s_output.split()[1:]

#write into df and write to s3 location:

count = 1
for line in dirs: 
    df = spark.read.text(line)
    if count < 10:
        df.write.mode("overwrite").format('text').option("compression", "gzip").save(f"s3a://gd-de-dp-pr-hcat-engagement-schema/event_jobsearch/dev-ds=1970-01-0{count}/")
    if count >= 10 and count < 32:
        df.write.mode("overwrite").format('text').option("compression", "gzip").save(f"s3a://gd-de-dp-pr-hcat-engagement-schema/event_jobsearch/dev-ds=1970-01-{count}/")
    if count >=32 and count < 40:
        df.write.mode("overwrite").format('text').option("compression", "gzip").save(f"s3a://gd-de-dp-pr-hcat-engagement-schema/event_jobsearch/dev-ds=1970-02-0{count%10-1}/")
    if count == 40:
        df.write.mode("overwrite").format('text').option("compression", "gzip").save(f"s3a://gd-de-dp-pr-hcat-engagement-schema/event_jobsearch/dev-ds=1970-02-09/")
    count += 1
