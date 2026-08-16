package connectors

type S3Connector struct {
	Bucket string
	Prefix string
}

func (c *S3Connector) Name() string { return "s3" }
