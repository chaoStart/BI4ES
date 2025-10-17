import torch
import numpy as np
from transformers import AutoModel, AutoTokenizer


class LLMEmbeddingModel():

    def __init__(self,
                 model_name_or_path,
                 batch_size=128,
                 max_length=1024,
                 gpu_id=0):
        """Local embedding model with automatic device selection"""
        self.model = AutoModel.from_pretrained(model_name_or_path, trust_remote_code=True)
        self.tokenizer = AutoTokenizer.from_pretrained(model_name_or_path, padding_side="right", trust_remote_code=True)

        # Device selection: CUDA -> MPS -> CPU
        if torch.cuda.is_available():
            self.device = torch.device(f"cuda:{gpu_id}")
        elif torch.backends.mps.is_available():
            self.device = torch.device("mps")
        else:
            self.device = torch.device("cpu")

        self.model.to(self.device).eval()

        self.max_length = max_length
        self.batch_size = batch_size

        query_instruction = "Given a search query, retrieve passages that answer the question"
        if query_instruction:
            self.query_instruction = f"Instruction: {query_instruction} \nQuery:"
        else:
            self.query_instruction = "Query:"

        self.doc_instruction = ""
        print(f"Model loaded: {model_name_or_path}")
        print(f"Device: {self.device}")

    def mean_pooling(self, hidden_state, attention_mask):
        s = torch.sum(hidden_state * attention_mask.unsqueeze(-1).float(), dim=1)
        d = attention_mask.sum(dim=1, keepdim=True).float()
        embedding = s / d
        return embedding

    @torch.no_grad()
    def encode(self, sentences_batch, instruction):
        inputs = self.tokenizer(
            sentences_batch,
            padding=True,
            truncation=True,
            return_tensors="pt",
            max_length=self.max_length,
            add_special_tokens=True,
        )
        # Move inputs to device
        inputs = {k: v.to(self.device) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = self.model(**inputs)
            last_hidden_state = outputs[0]

            instruction_tokens = self.tokenizer(
                instruction,
                padding=False,
                truncation=True,
                max_length=self.max_length,
                add_special_tokens=True,
            )["input_ids"]
            if len(np.shape(np.array(instruction_tokens))) == 1:
                inputs["attention_mask"][:, :len(instruction_tokens)] = 0
            else:
                instruction_length = [len(item) for item in instruction_tokens]
                assert len(instruction) == len(sentences_batch)
                for idx in range(len(instruction_length)):
                    inputs["attention_mask"][idx, :instruction_length[idx]] = 0

            embeddings = self.mean_pooling(last_hidden_state, inputs["attention_mask"])
            embeddings = torch.nn.functional.normalize(embeddings, dim=-1)
        return embeddings

    def encode_queries(self, queries):
        queries = queries if isinstance(queries, list) else [queries]
        queries = [f"{self.query_instruction}{query}" for query in queries]
        return self.encode(queries, self.query_instruction)

    def encode_passages(self, passages):
        passages = passages if isinstance(passages, list) else [passages]
        passages = [f"{self.doc_instruction}{passage}" for passage in passages]
        return self.encode(passages, self.doc_instruction)

    def compute_similarity_for_vectors(self, q_reps, p_reps):
        if len(p_reps.size()) == 2:
            return torch.matmul(q_reps, p_reps.transpose(0, 1))
        return torch.matmul(q_reps, p_reps.transpose(-2, -1))

    def compute_similarity(self, queries, passages):
        q_reps = self.encode_queries(queries)
        p_reps = self.encode_passages(passages)
        scores = self.compute_similarity_for_vectors(q_reps, p_reps)
        scores = scores.detach().cpu().tolist()
        return scores

    def display_results(self, query, passages, scores):
        """Display similarity results in a detailed visual format"""
        print("\n" + "=" * 80)
        print(f"🔍 Query: {query}")
        print("=" * 80)

        # Sort by similarity score (highest first)
        ranked_results = list(zip(passages, scores[0]))
        ranked_results.sort(key=lambda x: x[1], reverse=True)

        for i, (passage, score) in enumerate(ranked_results):
            # Rank indicators
            if i == 0:
                rank_indicator = "🥇 BEST MATCH"
                color = "\033[92m"  # Green
            elif i == 1:
                rank_indicator = "🥈 2nd BEST"
                color = "\033[93m"  # Yellow
            elif i == 2:
                rank_indicator = "🥉 3rd BEST"
                color = "\033[94m"  # Blue
            else:
                rank_indicator = f"#{i + 1}"
                color = "\033[90m"  # Gray

            # Relevance level
            if score > 0.6:
                relevance = "🔥 Highly Relevant"
            elif score > 0.3:
                relevance = "⚡ Moderately Relevant"
            elif score > 0.1:
                relevance = "💡 Weakly Relevant"
            else:
                relevance = "❌ Not Relevant"

            # Score bar visualization
            bar_length = int(score * 30)
            score_bar = "█" * bar_length + "░" * (30 - bar_length)

            print(f"\n{rank_indicator}")
            print(f"   Score: {color}{score:.4f}\033[0m | {relevance}")
            print(f"   Visual: [{score_bar}] {score * 100:.1f}%")
            print(f"   Content: \"{passage}\"")

        print("\n" + "=" * 80)


def get_youtu(queries, passages):
    queries = [queries]
    # passages = [
    #     '月发电量',
    #     "日发电量",
    #     '年发电量',
    #     '2025年发电量'
    # ]

    model_name_or_path = "D:\\git\\git_hub\\youtu-embedding-main\\youtu-model"
    model = LLMEmbeddingModel(model_name_or_path)
    scores = model.compute_similarity(queries, passages)

    # Display results with enhanced formatting
    model.display_results(queries[0], passages, scores)

    # Also show raw scores for reference
    print(f"\nRaw scores: {scores}")


# if __name__ == "__main__":
#     get_youtu()

