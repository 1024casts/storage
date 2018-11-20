package storage

// go test -bench .
// BenchmarkJsonMarshal-4     	  100000	     17623 ns/op
// BenchmarkJsonUnmarshal-4   	   20000	     94212 ns/op
// BenchmarkPBMarshal-4       	  500000	      2955 ns/op
// BenchmarkPBUnmarshal-4     	  300000	      3820 ns/op

import "testing"

type Int32SliceStructEncoding struct {
}

func (this Int32SliceStructEncoding) Marshal(v interface{}) ([]byte, error) {
	buf, err := v.(*Int32SliceStruct).Marshal()
	return buf, err
}

func (this Int32SliceStructEncoding) Unmarshal(data []byte, value interface{}) error {
	err := value.(*Int32SliceStruct).Unmarshal(data)
	return err
}

func BenchmarkMsgpack(b *testing.B) {
	// run the Fib function b.N times
	a := make([]int, 400)
	for i := 0; i < 400; i++ {
		a = append(a, i)
	}
	//jsonEncoding := JsonEncoding{}
	msgPackEncoding := MsgPackEncoding{}
	data, err := msgPackEncoding.Marshal(a)
	if err != nil {
		b.Error(err)
	}
	var result []int
	for n := 0; n < b.N; n++ {
		err = msgPackEncoding.Unmarshal(data, &result)
		if err != nil {
			b.Error(err)
		}

	}
}

func BenchmarkJsonMarshal(b *testing.B) {
	// run the Fib function b.N times
	//a := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	a := make([]int, 0, 400)
	for i := 0; i < 400; i++ {
		a = append(a, i)
	}
	jsonEncoding := JsonEncoding{}
	//msgPackEncoding := MsgPackEncoding{}
	for n := 0; n < b.N; n++ {
		_, err := jsonEncoding.Marshal(a)
		if err != nil {
			b.Error(err)
		}

	}
}

func BenchmarkJsonUnmarshal(b *testing.B) {
	// run the Fib function b.N times
	//a := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	a := make([]int, 0, 400)
	for i := 0; i < 400; i++ {
		a = append(a, i)
	}
	jsonEncoding := JsonEncoding{}
	//msgPackEncoding := MsgPackEncoding{}
	data, err := jsonEncoding.Marshal(a)
	if err != nil {
		b.Error(err)
	}
	var result []int
	for n := 0; n < b.N; n++ {
		err = jsonEncoding.Unmarshal(data, &result)
		if err != nil {
			b.Error(err)
		}

	}
}

func BenchmarkPBMarshal(b *testing.B) {
	// run the Fib function b.N times
	//a := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	a := make([]int32, 0, 400)
	for i := 0; i < 400; i++ {
		a = append(a, int32(i))
	}
	intSliceStructEncoding := Int32SliceStructEncoding{}
	intSliceStruct := Int32SliceStruct{
		Data: a,
	}
	for n := 0; n < b.N; n++ {
		_, err := intSliceStructEncoding.Marshal(&intSliceStruct)
		if err != nil {
			b.Error(err)
		}

	}
}

func BenchmarkPBUnmarshal(b *testing.B) {
	// run the Fib function b.N times
	//a := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
	a := make([]int32, 0, 400)
	for i := 0; i < 400; i++ {
		a = append(a, int32(i))
	}
	intSliceStructEncoding := Int32SliceStructEncoding{}
	intSliceStruct := Int32SliceStruct{
		Data: a,
	}
	data, err := intSliceStructEncoding.Marshal(&intSliceStruct)
	if err != nil {
		b.Error(err)
	}
	var result Int32SliceStruct
	for n := 0; n < b.N; n++ {
		err = intSliceStructEncoding.Unmarshal(data, &result)
		if err != nil {
			b.Error(err)
		}

	}
}
