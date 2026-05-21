package testutil

import "os"

func getenv(k string) string { return os.Getenv(k) }
