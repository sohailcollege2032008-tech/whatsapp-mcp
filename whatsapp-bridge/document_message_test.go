package main

import (
	"bytes"
	"path/filepath"
	"testing"

	"go.mau.fi/whatsmeow"
	waProto "go.mau.fi/whatsmeow/binary/proto"
	"google.golang.org/protobuf/proto"
)

func TestDocumentMetadataSurvivesWireEncoding(t *testing.T) {
	upload := whatsmeow.UploadResponse{
		URL: "https://example.invalid/document", DirectPath: "/document",
		MediaKey: []byte{1, 2}, FileSHA256: []byte{3, 4},
		FileEncSHA256: []byte{5, 6}, FileLength: 1234,
	}
	for _, tc := range []struct{ path, mime string }{
		{filepath.Join("private", "الكشف.pdf"), "application/pdf"},
		{filepath.Join("private", "FORM.PDF"), "application/pdf"},
		{filepath.Join("private", "الكشف.docx"), "application/vnd.openxmlformats-officedocument.wordprocessingml.document"},
		{filepath.Join("private", "الصور.xlsx"), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"},
		{filepath.Join("private", "unknown.unregistered-wa-format"), "application/octet-stream"},
		{filepath.Join("private", "extensionless"), "application/octet-stream"},
	} {
		t.Run(filepath.Base(tc.path), func(t *testing.T) {
			msg := &waProto.Message{DocumentMessage: buildDocumentMessage(tc.path, "caption", upload)}
			wire, err := proto.Marshal(msg)
			if err != nil {
				t.Fatal(err)
			}
			var decoded waProto.Message
			if err := proto.Unmarshal(wire, &decoded); err != nil {
				t.Fatal(err)
			}
			doc := decoded.GetDocumentMessage()
			if doc.GetMimetype() != tc.mime {
				t.Errorf("MIME = %q, want %q", doc.GetMimetype(), tc.mime)
			}
			if doc.GetFileName() != filepath.Base(tc.path) {
				t.Errorf("filename = %q, want basename", doc.GetFileName())
			}
			if doc.GetTitle() != filepath.Base(tc.path) || doc.GetCaption() != "caption" {
				t.Error("title or caption lost")
			}
			if doc.GetFileLength() != upload.FileLength || !bytes.Equal(doc.GetFileSHA256(), upload.FileSHA256) ||
				!bytes.Equal(doc.GetFileEncSHA256(), upload.FileEncSHA256) || !bytes.Equal(doc.GetMediaKey(), upload.MediaKey) ||
				doc.GetURL() != upload.URL || doc.GetDirectPath() != upload.DirectPath {
				t.Error("upload metadata changed")
			}
		})
	}
}
