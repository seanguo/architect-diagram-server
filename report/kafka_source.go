package report

type Source struct {
}

type KafkaSource struct {
	Source
}

func (s *KafkaSource) Progress() *Progress {
	return &Progress{}
}
