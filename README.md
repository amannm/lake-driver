# lake-driver
* JDBC driver for executing full-featured ANSI compliant SQL select statements on top of AWS S3 flat-file assets
* query optimization pushes column projection and filtering over to AWS ideally leading to less data transfer and buffering
* Most of the work is done by the Apache Calcite (and Apache Avatica) projects, via an interface called ProjectableFilterableTable that (work-in-progress) maps to the subset of SQL currently supported by "S3 Select" https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
* In-depth paper about Apache Calcite: https://arxiv.org/pdf/1802.10233.pdf

## environment setup
* make sure you've already used AWS CLI's configure command to add credentials to whatever environment you dev in
* in the test folder, replace all strings of "build.cauldron.tools" with an existing s3 bucket id of something your previously configured AWS credentials actually have read/write access to
* pass a list of TableSpecification defining all "external tables" your query needs to be a valid reference

## todo
* improve WHERE push-down
* explore usage of Java Flow API (Reactive Streams)
* performance profiling, optimization
* smarter, more comprehensive testing
