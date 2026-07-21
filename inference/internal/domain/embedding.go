package domain

type MultimodalContentItem struct {
	Type     string    `json:"type"`
	Text     string    `json:"text,omitempty"`
	ImageURL *ImageURL `json:"image_url,omitempty"`
}

type ImageURL struct {
	URL string `json:"url"`
}

type MultimodalInput struct {
	Content []MultimodalContentItem `json:"content"`
}

type EmbeddingRequest struct {
	Model          string            `json:"model"`
	Input          []MultimodalInput `json:"input"`
	EncodingFormat string            `json:"encoding_format"`
}

type EmbeddingDataItem struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type OpenRouterError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    int    `json:"code"`
}

type EmbeddingResponse struct {
	Object string              `json:"object"`
	Data   []EmbeddingDataItem `json:"data"`
	Model  string              `json:"model"`
	Error  *OpenRouterError    `json:"error,omitempty"`
}
