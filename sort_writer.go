package gossamr

import (
	"github.com/markchadwick/sortedpairs"
	"github.com/markchadwick/typedbytes"
	"io"
	"log"
)

type pairWriter struct {
	w io.Writer
}

func (pw *pairWriter) Write(k, v []byte) (err error) {
	if _, err = pw.w.Write(k); err != nil {
		return
	}
	_, err = pw.w.Write(v)
	return
}

type SortWriter struct {
	w   io.WriteCloser
	spw *sortedpairs.Writer
}

func NewSortWriter(w io.WriteCloser, capacity int) (*SortWriter, error) {
	pw := &pairWriter{w}
	spw, err := sortedpairs.NewWriter(pw, capacity)
	if err != nil {
		return nil, err
	}
	sw := &SortWriter{
		w:   w,
		spw: spw,
	}
	return sw, nil
}

func (sw *SortWriter) Write(k, v interface{}) (err error) {
	var kb, vb []byte
	if kb, err = typedbytes.Encode(k); err != nil {
		return
	}
	if vb, err = typedbytes.Encode(v); err != nil {
		return
	}
	return sw.spw.Write(kb, vb)
}

func (sw *SortWriter) Close() (err error) {
	err = sw.spw.Close()
	log.Printf("Closing sortwriter: %v", err)
	sw.w.Close()
	return
}

// type pair [2][]byte
//
// func (p pair) Write(w io.Writer) (err error) {
//   if err = p.write(w, 0); err != nil {
//     return
//   }
//   return p.write(w, 1)
// }
//
// func (p pair) write(w io.Writer, i int) (err error) {
//   bs := p[i]
//   if err = binary.Write(w, binary.BigEndian, int32(len(bs))); err != nil {
//     return err
//   }
//   _, err = w.Write(bs)
//   return
// }
//
//
// type pending [][2][]byte
//
// func (p pending) Len() int {
// 	return len(p)
// }
//
// func (p pending) Less(i, j int) bool {
// 	return bytes.Compare(p[i][0], p[j][0]) < 0
// }
//
// func (p pending) Swap(i, j int) {
// 	p[i], p[j] = p[j], p[i]
// }
//
// func (p pending) Write(w io.Writer) (err error) {
//   for _, pair := range p {
//     if err = p.WritePair(w, pair); err != nil {
//       return
//     }
//   }
//   return
// }
//
// func (p pending) WritePair(w io.Writer, pair [2][]byte) (err error) {
//   if err = p.WriteBytes(w, pair[0]); err != nil {
//     return
//   }
//   return p.WriteBytes(w, pair[1])
// }
//
// func (p pending) WriteBytes(w io.Writer, bs []byte) (err error) {
// }
//
// func ReadPairs(r io.Reader) (p pending, err error) {
//   p = make(pending, 0)
//   var key, value []byte
//
//   for {
//     if key, err = ReadBytes(r); err != nil {
//       return
//     }
//     if value, err = ReadBytes(r); err != nil {
//       return
//     }
//     p = append(p, [2][]byte{key, value})
//   }
//   return
// }
//
// func ReadBytes(r io.Reader) (b []byte, err error) {
//   var length int32
//   if err = binary.Read(r, binary.BigEndian, &length); err != nil {
//     return
//   }
//   b = make([]byte, length)
//   _, err = r.Read(b)
//   return
// }
//
// type SortWriter struct {
// 	Limit   int
// 	workdir string
// 	pending pending
// 	spillNo int
// }
//
// var _ Writer = new(SortWriter)
//
// func NewSortWriter(w Writer) (*SortWriter, error) {
// 	return NewLimitedSortWriter(w, 100)
// }
//
// func NewLimitedSortWriter(w Writer, limit int) (*SortWriter, error) {
// 	workdir, err := ioutil.TempDir("", "sort-writer-")
// 	if err != nil {
// 		return nil, err
// 	}
//
// 	sw := &SortWriter{
// 		Limit:   limit,
// 		workdir: workdir,
// 	}
// 	return sw, nil
// }
//
// func (sw *SortWriter) Write(k, v interface{}) (err error) {
// 	if len(sw.pending) >= sw.Limit {
// 		if err = sw.spill(); err != nil {
// 			return
// 		}
// 	}
// 	return sw.write(k, v)
// }
//
// func (sw *SortWriter) write(k, v interface{}) (err error) {
// 	var kb, vb []byte
// 	if kb, err = typedbytes.Encode(k); err != nil {
// 		return
// 	}
// 	if vb, err = typedbytes.Encode(v); err != nil {
// 		return
// 	}
// 	sw.pending = append(sw.pending, [2][]byte{kb, vb})
// 	return
// }
//
// func (sw *SortWriter) Close() error {
// 	if err := sw.spill(); err != nil {
// 		return err
// 	}
//
// 	if err := sw.merge(); err != nil {
// 		return err
// 	}
// 	return os.RemoveAll(sw.workdir)
// }
//
// func (sw *SortWriter) spill() (err error) {
// 	if len(sw.pending) == 0 {
// 		return nil
// 	}
//
// 	sort.Sort(sw.pending)
// 	f, err := sw.openSpill()
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()
//
//   if err = sw.pending.Write(f); err != nil {
//     return
//   }
//
// 	sw.spillNo++
// 	sw.pending = make(pending, 0)
// 	return nil
// }
//
// func (sw *SortWriter) openSpill() (*os.File, error) {
// 	fname := fmt.Sprintf("spill-%05d", sw.spillNo)
// 	path := path.Join(sw.workdir, fname)
// 	log.Printf("spilling to %s", path)
// 	return os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
// }
//
// func (sw *SortWriter) merge() error {
// 	sf, err := sw.splitFiles()
// 	if err != nil {
// 		return err
// 	}
// 	for len(sf) > 0 {
//
// 	}
// 	return nil
// }
//
// func (sw *SortWriter) splitFiles() ([]*splitFile, error) {
// 	splitFiles := make([]*splitFile, 0)
// 	fileInfos, err := ioutil.ReadDir(sw.workdir)
// 	if err != nil {
// 		return nil, err
// 	}
// 	for _, fi := range fileInfos {
// 		if !fi.IsDir() {
// 			file, err := os.Open(path.Join(sw.workdir, fi.Name()))
// 			if err != nil {
// 				return nil, err
// 			}
// 			splitFiles = append(splitFiles, newSplitFile(file))
// 		}
// 	}
// 	return splitFiles, nil
// }
//
// type splitFile struct {
// 	headKey   interface{}
// 	headValue interface{}
// 	pr        *typedbytes.PairReader
// }
//
// func newSplitFile(r io.Reader) *splitFile {
// 	pr := typedbytes.NewPairReader(typedbytes.NewReader(r))
// 	return &splitFile{
// 		pr: pr,
// 	}
// }
//
// func (sf *splitFile) head() (k, v interface{}, err error) {
// 	if sf.headKey == nil {
// 		sf.headKey, sf.headValue, err = sf.pr.Next()
// 		if err != nil {
// 			return nil, nil, err
// 		}
// 	}
// 	return sf.headKey, sf.headValue, nil
// }
//
// func (sf *splitFile) take() {
//   sf.headKey = nil
//   sf.headValue = nil
// }
