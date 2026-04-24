package version

var (
	Version = "dev"
	GitSHA  = "unknown"
)

func String() string {
	return Version + " (" + GitSHA + ")"
}
