package mount

import "os"

// writeAsBun writes contents to name as Bun's fs.writeFile does, which
// OpenCode saves with: it opens without O_TRUNC, writes, sets the size of
// what write(2) took and closes, and answers only what open(2) and write(2)
// answered.
func writeAsBun(name string, contents []byte) error {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE, 0o666)
	if err != nil {
		return err
	}
	written, err := file.Write(contents)
	_ = file.Truncate(int64(written))
	_ = file.Close()
	return err
}
