# lake-driver
* provides JDBC connections capable of executing SQL SELECT statements (see https://calcite.apache.org/docs/reference.html) on AWS S3 flat-file assets
* query optimization pushes column projection and filtering over to AWS (aka "predicate pushdown") leading to less data needing transfer out of S3 and reduced network/storage/memory footprint by the runtime computing+consuming the output record set
* Most of the work is done by the Apache Calcite (and Apache Avatica) projects, via an interface called ProjectableFilterableTable that (work-in-progress) maps to the subset of SQL currently supported by "S3 Select" https://docs.aws.amazon.com/AmazonS3/latest/dev/s3-glacier-select-sql-reference.html
* In-depth paper about Apache Calcite: https://arxiv.org/pdf/1802.10233.pdf

## environment setup
* make sure you've already used AWS CLI's configure command to add credentials to whatever environment you dev in
* in the test folder, replace all strings of "build.cauldron.tools" with an existing s3 bucket id of something your previously configured AWS credentials actually have read/write access to
* use the `LakeDriver.getConnection(...)` methods to create JDBC connections
  * pass a list of TableSpecification defining all "external tables" your query needs to be a valid reference
  * (optional) specify one of the following Scan classes to configure behavior
    * `LakeS3GetScan` uses GetObject, full tables are downloaded, both projection and filtering are performed in memory
    * `LakeS3SelectScan` (default) uses SelectObjectContent, only the required projected columns are downloaded, filtering is done in memory
    * `LakeS3SelectWhereScan` (experimental) uses SelectObjectContent, both projection and filtering is done on AWS, the results are downloaded, any remaining untranslated filters are applied in memory

## todo
* improve WHERE push-down
* performance profiling, optimization
* smarter, more comprehensive testing
* mixed scan mode: some table scans are better GET, others SELECT
* integrate and test the parquet compression support and save cash