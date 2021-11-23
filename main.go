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
	}

	UrlData struct {
		Contact uint32
		Action  uint64
		Time    int64
	}

	CopyControl struct {
		src *os.File
		dst *os.File
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

	if fh == nil {
		log.Fatal("Erro na leitura do arquivo")
	}

	// fecha o arquivo
	defer fh.Close()

	cc := new(CopyControl)

	cc.src = fh.fh
	cc.dst, err = os.OpenFile("temp_file.bin", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)

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
				log.Fatalf("Houve erro no retorno de dados! [%s]", err.Error())

				if errors.Is(err, io.EOF) {
					// caso seja final do arquivo, finaliza o script
					log.Fatalf("Fim do arquivo! [%s]", err.Error())
					os.Exit(1)
				}
			}

			var (
				// MsgpackHandle is a Handle for the Msgpack Schema-Free Encoding Format
				mh      codec.MsgpackHandle
				urlData UrlData
			)

			// NewDecoderString retorna um Decoder com decodes eficientes, diretamente de uma string com cópia zero.
			dec := codec.NewDecoderBytes(data, &mh)

			// A função Decode decodifica o stream de um leitor e aramazena o resultado um ponteiro de interface.
			err = dec.Decode(&urlData)

			if err != nil {
				fmt.Printf("Houve um erro ao fazer o decode com o MsgpackHandle: offset [%d],com o seguinte path [%s] e o erro [%s]\n", offset, binfile, err.Error())
				errorDec = true
				return true
			} else {
				fmt.Println("offset", offset)
				// Faz a cópia dos binários que são decodados, deixando de lado os que estão dando erro
				// para fazer a cópia vamos offset, destination
				// precisamos fazer o rename
				// cc.binaryCopy(int64(len(data)) + 4)
			}

			return true
		}()
		if !cont {
			break
		}
	}

	cc.dst.Close()
	cc.src.Close()
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
		fmt.Printf("Erro ao abrir o arquivo [%s]", err.Error())
		return nil, err
	}
	defer fh.Close()

	// verifica se o status do arquivo retorna erro,
	// se o offset é maior que o próprio tamanho do arquivo
	// ou se o erro é diferente de null
	if fi, err := fh.Stat(); offset >= fi.Size() || err != nil {
		fmt.Printf("Erro na validação do arquivo [%s]", err.Error())
		return nil, io.EOF
	}

	// utiliza o Seek para determinar a posição de onde o arquivo deve ser lido
	_, err = fh.Seek(offset, os.SEEK_SET)

	if err != nil {
		fmt.Printf("Erro no seek: [%s]", err.Error())
		return nil, err
	}

	// cria um slice de tamanho 4
	// sera armazenado os 4 primeiros bytes, o offset que determina o tamanho do que será lido
	b := make([]byte, 4)

	// faz a leitura dos bytes e aloca no slice determinado
	_, err = fh.Read(b)

	if err != nil {
		fmt.Printf("Erro ao realizar a leitura do arquivo: [%s]", err.Error())
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

// CopyBinary
func (f *FileControl) CopyBinary(offset int64) (int64, error) {
	var bytes int64

	src, err := os.Open(f.path)

	if err != nil {
		fmt.Printf("Erro ao abrir o arquivo [%s]", err.Error())
		return 0, err
	}

	// vamos criar o arquivo temporário que irá gravar apenas os offsets que não estão dando problema algum
	dst, err := os.OpenFile("temp_file.bin", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0755)

	if err != nil {
		return 0, err
	}

	// fecha o arquivo de destino
	defer dst.Close()

	bin := make([]byte, offset)
	_, err = src.Read(bin)

	bytes, err = io.CopyBuffer(dst, src, bin)

	if err != nil {
		return 0, err
	}

	// fechamento do primeiro arquivo, arquivo source
	src.Close()
	return bytes, nil
}

//
func (cc *CopyControl) binaryCopy(offset int64) (int64, error) {
	var bytes int64

	bin := make([]byte, offset)
	_, err := cc.src.Read(bin)

	bytes, err = io.CopyBuffer(cc.dst, cc.src, bin)

	if err != nil {
		return 0, err
	}

	return bytes, nil
}
