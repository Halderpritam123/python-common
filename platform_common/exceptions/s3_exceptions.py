class S3BucketNotFound(Exception):
    def __init__(self, bucket) -> None:
        self.message = f"Bucket {bucket} does not exist"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class S3BucketAccessDenied(Exception):
    def __init__(self, bucket) -> None:
        self.message = f"Access denied for {bucket}"
        super().__init__(self.message)
    def __str__(self):
        return self.message
class S3ObjectNotFound(Exception):
    def __init__(self, object) -> None:
        self.message = f"File not found: {object}"
        super().__init__(self.message)
    def __str__(self):
        return self.message