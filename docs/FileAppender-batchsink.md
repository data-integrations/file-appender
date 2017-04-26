# File Appender Batch Sink

Description
-----------

Writes to a CDAP FileSet in text format. HDFS append must be enabled for this to work.
One line is written for each record sent to the sink.
All record fields are joined using a configurable separator.
Each time a batch is written, the sink will examine all existing files in the output directory.
If there are any files that are smaller in size than the size threshold, or more recent than
the age threshold, new data will be appended to those files instead of written to new files.

Properties
----------

**name:** The name of the FileSet to write to.

**fieldSeparator:** The separator to join input record fields on. Defaults to ','.

**outputDir:** The output directory to write to.

**fileNamePrefix:** The prefix for all files written to the FileSet

**sizeThreshold:** Files greater than this size (in megabytes) will be closed and a new file will be written to.
0 means there is no threshold. Defaults to 100.

**ageThreshold:** Files older than this size (in minutes) will not be appended to.
0 means there is no threshold. Defaults to 60.

**schema:** Optional schema of the FileSet. If not given, the input schema into the stage will be used.
