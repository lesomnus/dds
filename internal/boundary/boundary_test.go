// Package boundary checks that DDS stays DDS: the packages that implement it
// do not depend on ROS 2. What is ROS's -- names, nodes, message types --
// lives in ros2 (and cdr/ros), on top.
package boundary

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

const module = "github.com/lesomnus/dds"

// core is DDS: none of it may import, however indirectly, what is ROS's.
var core = []string{"rtps", "discovery", "participant", "transport", "traffic", "cdr", "internal/pcap"}

var ros = []string{"ros2", "cdr/ros"}

// imports are the module's packages a package's non-test files import.
func imports(t *testing.T, root, pkg string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(root, pkg, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		af, err := parser.ParseFile(token.NewFileSet(), f, nil, parser.ImportsOnly)
		if err != nil {
			t.Fatal(err)
		}
		for _, im := range af.Imports {
			path, _ := strconv.Unquote(im.Path.Value)
			if rel, ok := strings.CutPrefix(path, module+"/"); ok {
				out = append(out, rel)
			}
		}
	}
	return out
}

func TestCoreDoesNotDependOnROS(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "go.mod")); err != nil {
		t.Fatal(err)
	}
	for _, start := range core {
		// Walk what it imports, remembering the way, to say how it got there.
		via := map[string]string{start: ""}
		queue := []string{start}
		for len(queue) > 0 {
			pkg := queue[0]
			queue = queue[1:]
			for _, r := range ros {
				if pkg == r || strings.HasPrefix(pkg, r+"/") {
					path := pkg
					for p := via[pkg]; p != ""; p = via[p] {
						path = p + " -> " + path
					}
					t.Errorf("%s depends on ROS: %s", start, path)
				}
			}
			for _, im := range imports(t, root, pkg) {
				if _, seen := via[im]; !seen {
					via[im] = pkg
					queue = append(queue, im)
				}
			}
		}
	}
}
