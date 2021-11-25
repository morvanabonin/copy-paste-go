package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"sync"

	"github.com/ugorji/go/codec"
)

type (
	FileControl struct {
		currpos int64
		path    string
		fh      *os.File
		created bool
		sync.Mutex
		dst *os.File
	}

	UrlData struct {
		Contact uint32
		Action  uint64
		Time    int64
	}
)

var binfile string

func main() {

	defer func() {
		if r := recover(); r != nil {
			stack := debug.Stack()
			fmt.Println("Recuperado de ", r)
			fmt.Println("Stack ", string(stack))
			return
		}
	}()

	// pega o path do arquivo que deve ser lido
	flag.StringVar(&binfile, "binFile", "", "file to be processed")
	flag.Parse()

	//verifica se o arquivo existe, foi passado
	if (binfile) == "" {
		log.Fatal("Arquivo não existe")
	}

	err := Reader(binfile)

	if err != nil {
		log.Fatal("Houve erro ao ler e decodar o arquivo binário", err.Error())
	}
}

func Reader(binfile string) error {
	var err error

	fh := NewFileReader(filepath.Clean(binfile))
	dst := filepath.Dir(fh.path) + "/temp_file.bin"
	fh.dst, err = os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)

	if fh == nil {
		log.Fatal("Erro na leitura do arquivo")
	}

	// fecha os arquivos
	defer fh.Close()
	defer fh.dst.Close()

	if err != nil {
		return err
	}

	// define o inicio do offset
	offset := int64(0)
	// offset := int64(3645531)

	for {
		cont := func() bool {
			errorDec := false

			// slice de bytes (entre os len 34/36) com as informações que
			// serão convertidas em uma estrutura
			data, err := fh.Read(offset)

			// ao final da função pega e próximo offset
			// o tamanho do slice de bytes de dados + a posição dos próximos
			defer func() {
				if errorDec {
					offset++
				} else {
					offset += int64(len(data)) + 4
				}
			}()

			if err != nil {

				if errors.Is(err, io.EOF) {

					// caso seja final do arquivo, finaliza o script e renomeia
					RenameFile(fh.path, dst)
					fmt.Printf("Fim do arquivo! [%s]", err.Error())
				}

				log.Fatalf("Houve erro no retorno de dados! [%s]", err.Error())
			}

			var (
				// MsgpackHandle is a Handle for the Msgpack Schema-Free Encoding Format
				mh      codec.MsgpackHandle
				urlData UrlData
			)

			// NewDecoderString retorna um Decoder com decodes eficientes, diretamente de uma string com cópia zero.
			dec := codec.NewDecoderBytes(data, &mh)

			// A função Decode decodifica o stream de um leitor e aramazena o resultado em um ponteiro de interface.
			err = dec.Decode(&urlData)

			if err != nil {
				fmt.Printf("Houve um erro ao fazer o decode com o MsgpackHandle: offset [%d],com o seguinte path [%s] e o erro [%s]\n", offset, binfile, err.Error())
				errorDec = true
				return true
			} else {
				// Faz a cópia dos binários que são decodados, deixando de lado os que estão dando erro
				// para fazer a cópia vamos precisar offset, destination
				// precisamos fazer o rename
				fh.binaryCopy(offset, int64(len(data))+4)
			}

			return true
		}()
		if !cont {
			break
		}
	}

	return nil
}

func NewFileReader(path string) *FileControl {
	fh, err := os.OpenFile(path, os.O_RDONLY, 0755)
	if err != nil {
		fmt.Printf("Erro ao abrir o arquivo %s", err.Error())
		return nil
	}

	fc := &FileControl{fh: fh, path: path, created: true}
	return fc
}

func (f *FileControl) Close() error {
	f.created = false
	return f.fh.Close()
}

func (f *FileControl) Read(offset int64) ([]byte, error) {
	fh, err := os.Open(f.path)

	// caso o erro retorne diferente de nil
	if err != nil {
		fmt.Printf("Erro ao abrir o arquivo [%v]", err)
		return nil, err
	}
	defer fh.Close()

	// verifica se o status do arquivo retorna erro,
	// se o offset é maior que o próprio tamanho do arquivo
	// ou se o erro é diferente de null
	if fi, err := fh.Stat(); offset >= fi.Size() || err != nil {
		return nil, io.EOF
	}

	// utiliza o Seek para determinar a posição de onde o arquivo deve ser lido
	_, err = fh.Seek(offset, os.SEEK_SET)

	if err != nil {
		fmt.Printf("Erro no seek: [%v]", err)
		return nil, err
	}

	// cria um slice de tamanho 4
	// sera armazenado os 4 primeiros bytes, o offset que determina o tamanho do que será lido
	b := make([]byte, 4)

	// faz a leitura dos bytes e aloca no slice determinado
	_, err = fh.Read(b)

	if err != nil {
		fmt.Printf("Erro ao realizar a leitura do arquivo: [%v]", err)
		return nil, err
	}

	// faz a conversão dos de binary para Int32
	// o tamanho é definição dos próximos bytes que serão lidos
	v := BinaryToInt32(b)

	if v < 0 || v > 200 {
		return nil, err
	}

	// criaçõo do slice de bytes com tamanho determinado pelo offset
	buffer := make([]byte, v)

	// alocação no slice de bystes de tamanho determinado
	_, err = fh.Read(buffer)

	return buffer, err
}

// BinaryToInt32 função qque faz a conversão de bytes para int32
func BinaryToInt32(b []byte) (val int32) {
	buf := bytes.NewBuffer(b)
	err := binary.Read(buf, binary.LittleEndian, &val)

	if err != nil {
		return 0
	}
	return val
}

// binaryCopy faz a cópia dos binários funcionais para um arquivo temporário e ignora os corrompidos
func (fc *FileControl) binaryCopy(offset int64, binary_size int64) (int, error) {
	var bytes int

	src, err := os.Open(fc.path)

	if err != nil {
		fmt.Printf("Erro ao abrir o arquivo [%s]", err.Error())
		return 0, err
	}

	defer src.Close()

	_, err = src.Seek(offset, os.SEEK_SET)
	bin := make([]byte, binary_size)
	_, err = src.Read(bin)

	bytes, err = fc.dst.Write(bin)

	if err != nil {
		return 0, err
	}

	return bytes, nil
}

func RenameFile(src, dst string) {
	// Arquivo de entrada e arquivo de saída

	if _, err := os.Stat(src); err != nil {
		// The source does not exist or some other error accessing the source
		log.Fatal("Erro ao verificar o arquivo de entrada:", err)
	}
	if _, err := os.Stat(dst); err != nil {
		// The destination exists or some other error accessing the destination
		log.Fatal("Erro ao verificar o arquivo temporário:", err)
	}
	if err := os.Rename(dst, src); err != nil {
		log.Fatal(err)
	}

}
