package minio

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"os"

	"strconv"
	"strings"

	"net/http"

	"github.com/minio/cli"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/minio/minio-go/v7/pkg/encrypt"
)

var debugClient *minio.Core

func handleOutput(v interface{}) {
	if vstr, ok := v.(string); ok {
		os.Stdout.WriteString(vstr)
	} else {
		out, _ := json.MarshalIndent(v, "", "  ")
		os.Stdout.Write(out)
	}
}

// Debug - entry.
func Debug() {
	app := cli.NewApp()
	app.Usage = "Minio debugger"
	app.Author = "Minio.io"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "endpoint",
		},
		cli.StringFlag{
			Name: "accesskey",
		},
		cli.StringFlag{
			Name: "secretkey",
		},
		cli.BoolFlag{
			Name: "secure",
		},
		cli.BoolFlag{
			Name: "trace",
		},
	}
	app.Before = debugMain
	app.Commands = []cli.Command{
		{
			Name:  "multipart",
			Usage: "Multipart related operations",
			Action: func(ctx *cli.Context) {
				fmt.Println("here")
				cli.ShowCommandHelp(ctx, "")
			},
			Subcommands: []cli.Command{
				{
					Name:   "new",
					Usage:  "New multipart upload",
					Action: debugNewMultipart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "bucket",
							Usage: "Bucket name",
						},
						cli.StringFlag{
							Name:  "object",
							Usage: "Object name",
						},
						cli.BoolFlag{
							Name:  "encrypt",
							Usage: "encrypt the object",
						},
					},
				},
				{
					Name:   "upload",
					Usage:  "Upload part",
					Action: debugUploadPart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "bucket",
							Usage: "Bucket name",
						},
						cli.StringFlag{
							Name:  "object",
							Usage: "Object name",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
						cli.IntFlag{
							Name: "number",
						},
						cli.StringFlag{
							Name: "file",
						},
					},
				},
				{
					Name:   "complete",
					Usage:  "Complete multipart",
					Action: debugCompleteMultipart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
					},
				},
				{
					Name:   "listuploads",
					Usage:  "List incomplete uploads",
					Action: debugListMultipartUploads,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "prefix",
						},
						cli.StringFlag{
							Name: "keymarker",
						},
						cli.StringFlag{
							Name: "uploadidmarker",
						},
						cli.BoolFlag{
							Name: "delimiter",
						},
						cli.IntFlag{
							Name: "maxuploads",
						},
					},
				},
				{
					Name:   "listparts",
					Usage:  "List parts",
					Action: debugListUploadParts,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
						cli.IntFlag{
							Name: "partmarker",
						},
						cli.IntFlag{
							Name: "maxparts",
						},
					},
				},
				{
					Name:   "abort",
					Usage:  "Abort multipart upload",
					Action: debugAbortMultipartUpload,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
					},
				},
			},
		},
	}
	app.RunAndExitOnError()
}

type minioContext struct {
	Endpoint      string
	AccessKey     string
	SecretKey     string
	Secure, Trace bool
}

func newMinioContext(ctx *cli.Context) minioContext {
	minioCtx := minioContext{}
	minioCtx.Endpoint = ctx.GlobalString("endpoint")
	if minioCtx.Endpoint == "" {
		minioCtx.Endpoint = os.Getenv("ENDPOINT")
	}
	minioCtx.AccessKey = ctx.GlobalString("accesskey")
	if minioCtx.AccessKey == "" {
		minioCtx.AccessKey = os.Getenv("ACCESS_KEY")
	}
	minioCtx.SecretKey = ctx.GlobalString("secretkey")
	if minioCtx.SecretKey == "" {
		minioCtx.SecretKey = os.Getenv("SECRET_KEY")
	}
	minioCtx.Trace = ctx.GlobalBool("trace") || os.Getenv("TRACE") == "1"
	minioCtx.Secure = ctx.GlobalBool("secure") || os.Getenv("SECURE") == "1"
	return minioCtx
}

func debugMain(ctx *cli.Context) error {
	minioCtx := newMinioContext(ctx)
	transport := http.DefaultTransport
	// if minioCtx.Trace {
	// 	transport = httptracer.GetNewTraceTransport(newTraceV4(), http.DefaultTransport)
	// }
	var err error
	debugClient, err = minio.NewCore(minioCtx.Endpoint, &minio.Options{
		Creds:     credentials.NewStaticV4(minioCtx.AccessKey, minioCtx.SecretKey, ""),
		Secure:    minioCtx.Secure,
		Transport: transport,
	})
	if err != nil {
		fmt.Println(err)
		cli.ShowCommandHelp(ctx, "")
		return err
	}
	return nil
}

func debugNewMultipart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	var sse encrypt.ServerSide
	if ctx.Bool("encrypt") {
		sse = encrypt.NewSSE()
	}
	result, err := debugClient.NewMultipartUpload(context.Background(), bucketName, objectName, minio.PutObjectOptions{
		ServerSideEncryption: sse,
	})
	if err != nil {
		log.Fatal(err)
		cli.ShowCommandHelp(ctx, "")
	}
	handleOutput(result)
}

func debugUploadPart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	partNum := ctx.Int("number")
	filePath := ctx.String("file")
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	fi, _ := f.Stat()
	part, err := debugClient.PutObjectPart(context.Background(), bucketName, objectName, uploadID, partNum, f, fi.Size(), minio.PutObjectPartOptions{})
	if err != nil {
		log.Fatal(err)
	}
	handleOutput(part)
}

func debugCompleteMultipart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	parts := ctx.Args()
	var uparts []minio.CompletePart
	for _, part := range parts {
		split := strings.Split(part, ".")
		partNum, err := strconv.Atoi(split[0])
		if err != nil {
			log.Fatal(err)
		}
		md5sum := split[1]
		uparts = append(uparts, minio.CompletePart{
			PartNumber: partNum,
			ETag:       md5sum,
		})
	}
	result, err := debugClient.CompleteMultipartUpload(context.Background(), bucketName, objectName, uploadID, uparts, minio.PutObjectOptions{})
	if err != nil {
		log.Fatal(err)
	}
	handleOutput(result)
}

func debugListMultipartUploads(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	prefix := ctx.String("prefix")
	keyMarker := ctx.String("keymarker")
	uploadIDMarker := ctx.String("uploadidmarker")
	delimiter := ""
	maxuploads := ctx.Int("maxuploads")
	if ctx.Bool("delimiter") {
		delimiter = "/"
	}
	result, err := debugClient.ListMultipartUploads(context.Background(), bucketName, keyMarker, uploadIDMarker, prefix, delimiter, maxuploads)
	if err != nil {
		log.Fatal(err)
	}
	handleOutput(result)
}

func debugListUploadParts(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	partmarker := ctx.Int("partmarker")
	maxParts := ctx.Int("maxparts")
	result, err := debugClient.ListObjectParts(context.Background(), bucketName, objectName, uploadID, partmarker, maxParts)
	if err != nil {
		log.Fatal(err)
	}
	handleOutput(result)
}

func debugAbortMultipartUpload(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	err := debugClient.AbortMultipartUpload(context.Background(), bucketName, objectName, uploadID)
	if err != nil {
		handleOutput(struct {
			Status bool
			Msg    string
		}{false, err.Error()})
		return
	}
	handleOutput(struct{ Status bool }{true})
}
