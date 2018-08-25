# lake-driver
calcite-based JDBC driver for cheaply executing SQL queries on Amazon S3


##environment setup
* make sure you've already used AWS CLI's configure command to add credentials to whatever environment you dev in
* in the test folder, replace all strings of "build.cauldron.tools" with an existing s3 bucket id of something your previously configured AWS credentials actually have read/write access to