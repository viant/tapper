package emitter

import (
	"fmt"
	"path"
	"time"

	"github.com/viant/afs/url"
)

const (
	//DestPath dest path
	DestPath = "$DestPath"
	//Dest dest location
	Dest = "$Dest"
	//DestName dest simple name
	DestName = "$DestName"
	//TimePath stream create time based path fragment
	TimePath = "$TimePath"
)

func expandParameters(params map[string]string, destPath string, created time.Time) map[string]string {
	var result = make(map[string]string)
	for key, value := range params {
		switch value {
		case DestPath:
			result[key] = url.Path(destPath)
		case Dest:
			result[key] = destPath
		case DestName:
			destPath := url.Path(destPath)
			_, result[key] = path.Split(destPath)
		case TimePath:
			result[key] = fmt.Sprintf("%d/%02d/%02d/%02d", created.Year(), created.Month(), created.Day(), created.Hour())
		}
	}
	return result
}

func expandArguments(args []string, destPath string, created time.Time) []string {
	var result = make([]string, len(args))
	for i, item := range args {
		result[i] = item
		switch item {
		case DestPath:
			result[i] = url.Path(destPath)
		case Dest:
			result[i] = destPath
		case DestName:
			destPath := url.Path(destPath)
			_, result[i] = path.Split(destPath)
		case TimePath:
			result[i] = fmt.Sprintf("%d/%02d/%02d/%02d", created.Year(), created.Month(), created.Day(), created.Hour())
		}
	}
	return result
}
