package connectors

// S3Connector is a built-in connector surface for S3 source/sink interactions.
type S3Connector struct {
	Bucket string
	Prefix string
}

func (c *S3Connector) Name() string { return "s3" }
