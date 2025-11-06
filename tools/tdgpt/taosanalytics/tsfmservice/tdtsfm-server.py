
import argparse
import torch
from torch import nn
import torch.nn.functional as F

from transformers import PreTrainedModel, Cache, DynamicCache
from transformers.activations import ACT2FN
from transformers.modeling_attn_mask_utils import _prepare_4d_causal_attention_mask
from transformers.modeling_outputs import MoeModelOutputWithPast, MoeCausalLMOutputWithPast
from transformers import PretrainedConfig
from typing import Any, Dict, List, Optional, Union, Callable, Tuple
from transformers import GenerationMixin, LogitsProcessorList, StoppingCriteriaList
from transformers.generation import validate_stopping_criteria, EosTokenCriteria
from transformers.generation.utils import GenerateNonBeamOutput, GenerateEncoderDecoderOutput, GenerateDecoderOnlyOutput, GenerationConfig, GenerateOutput
from transformers.utils import ModelOutput
from flask import Flask, request, jsonify


app = Flask(__name__)

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
max_len = 2048
Taos_model = None

class TaosConfig(PretrainedConfig):
    model_type = "taos"
    keys_to_ignore_at_inference = ["Taos_past_key_values"]

    def __init__(
        self,
        Taos_input_token_len: int = 1,
        Taos_hidden_size: int = 1024,
        Taos_intermediate_size: int = 2048,
        Taos_output_token_lens: List[int] = [1, 8, 32, 64],
        Taos_num_hidden_layers: int = 8,
        Taos_num_attention_heads: int = 8,
        Taos_hidden_act: str = "silu",
        Taos_use_cache: bool = True,
        Taos_rope_theta: int = 10000,
        Taos_attention_dropout: float = 0.0,
        Taos_initializer_range: float = 0.02,
        Taos_max_position_embeddings: int = 10000,
        **kwargs,
    ):
        self.Taos_input_token_len = Taos_input_token_len
        self.Taos_hidden_size = Taos_hidden_size
        self.Taos_intermediate_size = Taos_intermediate_size
        self.Taos_num_hidden_layers = Taos_num_hidden_layers
        self.Taos_num_attention_heads = Taos_num_attention_heads
        self.Taos_hidden_act = Taos_hidden_act
        self.Taos_output_token_lens = Taos_output_token_lens
        self.Taos_use_cache = Taos_use_cache
        self.Taos_rope_theta = Taos_rope_theta
        self.Taos_attention_dropout = Taos_attention_dropout
        self.Taos_initializer_range = Taos_initializer_range
        self.Taos_max_position_embeddings = Taos_max_position_embeddings
        super().__init__(**kwargs)

class TaosTSGenerationMixin(GenerationMixin):
    @torch.no_grad()
    def generate(
        self,
        inputs: Optional[torch.Tensor] = None,
        generation_config: Optional[GenerationConfig] = None,
        logits_processor: Optional[LogitsProcessorList] = None,
        stopping_criteria: Optional[StoppingCriteriaList] = None,
        prefix_allowed_tokens_fn: Optional[Callable[[int, torch.Tensor], List[int]]] = None,
        synced_gpus: Optional[bool] = None,
        assistant_model: Optional["PreTrainedModel"] = None,
        streamer = None,
        negative_prompt_ids: Optional[torch.Tensor] = None,
        negative_prompt_attention_mask: Optional[torch.Tensor] = None,
        **kwargs,
    ) -> Union[GenerateOutput, torch.LongTensor]:
        if len(inputs.shape) == 2:
            batch_size, cur_len = inputs.shape
            if cur_len < self.config.Taos_input_token_len:
                raise ValueError(f"Input length must be at least {self.config.Taos_input_token_len}")
            elif cur_len % self.config.Taos_input_token_len != 0:
                new_len = (cur_len // self.config.Taos_input_token_len) * self.config.Taos_input_token_len
                inputs = inputs[:, -new_len:]
        else:
            raise ValueError('Input shape must be: [batch_size, seq_len]')
        return super().generate(inputs=inputs, generation_config=generation_config, logits_processor=logits_processor, stopping_criteria=stopping_criteria, prefix_allowed_tokens_fn=prefix_allowed_tokens_fn, synced_gpus=synced_gpus, assistant_model=assistant_model, streamer=streamer, negative_prompt_ids=negative_prompt_ids, negative_prompt_attention_mask=negative_prompt_attention_mask, **kwargs)

    def _greedy_search(
            self,
            input_ids: torch.Tensor,
            logits_processor: Optional[LogitsProcessorList] = None,
            stopping_criteria: Optional[StoppingCriteriaList] = None,
            max_length: Optional[int] = None,
            pad_token_id: Optional[int] = None,
            eos_token_id: Optional[Union[int, List[int]]] = None,
            output_attentions: Optional[bool] = None,
            output_hidden_states: Optional[bool] = None,
            output_scores: Optional[bool] = None,
            output_logits: Optional[bool] = None,
            return_dict_in_generate: Optional[bool] = None,
            synced_gpus: bool = False,
            streamer = None,
            **model_kwargs,
    ) -> Union[GenerateNonBeamOutput, torch.Tensor]:
        input_ids = input_ids.to(self.device)
        batch_size, cur_len = input_ids.shape
        logits_processor = logits_processor if logits_processor is not None else LogitsProcessorList()
        stopping_criteria = stopping_criteria if stopping_criteria is not None else StoppingCriteriaList()
        if max_length is not None:
            stopping_criteria = validate_stopping_criteria(stopping_criteria, max_length)
        pad_token_id = pad_token_id if pad_token_id is not None else self.generation_config.pad_token_id
        if eos_token_id is not None:
            stopping_criteria.append(EosTokenCriteria(eos_token_id=eos_token_id))
        if isinstance(eos_token_id, int):
            eos_token_id = [eos_token_id]
        output_scores = output_scores if output_scores is not None else self.generation_config.output_scores
        output_attentions = output_attentions if output_attentions is not None else self.generation_config.output_attentions
        output_hidden_states = output_hidden_states if output_hidden_states is not None else self.generation_config.output_hidden_states
        return_dict_in_generate = return_dict_in_generate if return_dict_in_generate is not None else self.generation_config.return_dict_in_generate

        raw_logits = () if (return_dict_in_generate and output_logits) else None
        scores = () if (return_dict_in_generate and output_scores) else None
        decoder_attentions = () if (return_dict_in_generate and output_attentions) else None
        cross_attentions = () if (return_dict_in_generate and output_attentions) else None
        decoder_hidden_states = () if (return_dict_in_generate and output_hidden_states) else None

        if return_dict_in_generate and self.config.is_encoder_decoder:
            encoder_attentions = model_kwargs["encoder_outputs"].get("attentions") if output_attentions else None
            encoder_hidden_states = model_kwargs["encoder_outputs"].get("hidden_states") if output_hidden_states else None

        if "inputs_embeds" in model_kwargs:
            cur_len = model_kwargs["inputs_embeds"].shape[1]
        this_peer_finished = False
        unfinished_sequences = torch.ones(batch_size, dtype=torch.long, device=input_ids.device)
        model_kwargs["cache_position"] = torch.arange(cur_len, device=input_ids.device)
        true_seq_len = cur_len // self.config.Taos_input_token_len
        model_kwargs["attention_mask"] = model_kwargs["attention_mask"][:, -true_seq_len:]
        max_length = stopping_criteria.max_length
        while self._has_unfinished_sequences(this_peer_finished, synced_gpus, device=input_ids.device):
            model_inputs = self.prepare_inputs_for_generation(input_ids, **model_kwargs)
            input_length = input_ids.shape[1]
            outputs = self(
                **model_inputs,
                return_dict=True,
                output_attentions=output_attentions,
                output_hidden_states=output_hidden_states,
                max_output_length=max_length - input_length,
            )
            if synced_gpus and this_peer_finished:
                continue
            next_token_logits = outputs.logits
            next_tokens_scores = logits_processor(input_ids, next_token_logits)
            if return_dict_in_generate:
                if output_scores:
                    scores += (next_tokens_scores,)
                if output_logits:
                    raw_logits += (next_token_logits,)
                if output_attentions:
                    decoder_attentions += ((outputs.decoder_attentions,) if self.config.is_encoder_decoder else (outputs.attentions,))
                    if self.config.is_encoder_decoder:
                        cross_attentions += (outputs.cross_attentions,)
                if output_hidden_states:
                    decoder_hidden_states += ((outputs.decoder_hidden_states,) if self.config.is_encoder_decoder else (outputs.hidden_states,))
            next_tokens = next_tokens_scores
            if eos_token_id is not None:
                if pad_token_id is None:
                    raise ValueError("If `eos_token_id` is defined, make sure that `pad_token_id` is defined.")
                next_tokens = next_tokens * unfinished_sequences + pad_token_id * (1 - unfinished_sequences)
            horizon_length = next_tokens.shape[1] // self.config.Taos_input_token_len
            input_ids = torch.cat([input_ids, next_tokens], dim=-1)
            if streamer is not None:
                streamer.put(next_tokens.cpu())
            model_kwargs = self._update_model_kwargs_for_generation(
                outputs,
                model_kwargs,
                horizon_length=horizon_length,
                is_encoder_decoder=self.config.is_encoder_decoder,
            )
            unfinished_sequences = unfinished_sequences & ~stopping_criteria(input_ids, scores)
            this_peer_finished = unfinished_sequences.max() == 0
        if input_ids.shape[1] > max_length:
            input_ids = input_ids[:, :max_length]
        if streamer is not None:
            streamer.end()
        if return_dict_in_generate:
            if self.config.is_encoder_decoder:
                return GenerateEncoderDecoderOutput(
                    sequences=input_ids,
                    scores=scores,
                    logits=raw_logits,
                    encoder_attentions=encoder_attentions,
                    encoder_hidden_states=encoder_hidden_states,
                    decoder_attentions=decoder_attentions,
                    cross_attentions=cross_attentions,
                    decoder_hidden_states=decoder_hidden_states,
                    past_key_values=model_kwargs.get("Taos_past_key_values"),
                )
            else:
                return GenerateDecoderOnlyOutput(
                    sequences=input_ids,
                    scores=scores,
                    logits=raw_logits,
                    attentions=decoder_attentions,
                    hidden_states=decoder_hidden_states,
                    past_key_values=model_kwargs.get("Taos_past_key_values"),
                )
        else:
            return input_ids[:, -(max_length - cur_len):]

    def _update_model_kwargs_for_generation(
            self,
            outputs: ModelOutput,
            model_kwargs: Dict[str, Any],
            horizon_length: int = 1,
            is_encoder_decoder: bool = False,
            standardize_cache_format: bool = False,
    ) -> Dict[str, Any]:
        model_kwargs["Taos_past_key_values"] = self._extract_past_from_model_output(outputs, standardize_cache_format=standardize_cache_format)
        if getattr(outputs, "state", None) is not None:
            model_kwargs["state"] = outputs.state
        if "token_type_ids" in model_kwargs:
            token_type_ids = model_kwargs["token_type_ids"]
            model_kwargs["token_type_ids"] = torch.cat([token_type_ids, token_type_ids[:, -1].unsqueeze(-1)], dim=-1)
        if not is_encoder_decoder:
            if "attention_mask" in model_kwargs:
                attention_mask = model_kwargs["attention_mask"]
                model_kwargs["attention_mask"] = torch.cat([attention_mask, attention_mask.new_ones((attention_mask.shape[0], horizon_length))], dim=-1)
        else:
            if "decoder_attention_mask" in model_kwargs:
                decoder_attention_mask = model_kwargs["decoder_attention_mask"]
                model_kwargs["decoder_attention_mask"] = torch.cat([decoder_attention_mask, decoder_attention_mask.new_ones((decoder_attention_mask.shape[0], horizon_length))], dim=-1)
        if "cache_position" in model_kwargs and model_kwargs["cache_position"] is not None:
            model_kwargs["cache_position"] = model_kwargs["cache_position"][-1:] + horizon_length
        return model_kwargs

def rotate_half(x):
    x1 = x[..., : x.shape[-1] // 2]
    x2 = x[..., x.shape[-1] // 2:]
    return torch.cat((-x2, x1), dim=-1)

def apply_rotary_pos_emb(q, k, cos, sin, position_ids, unsqueeze_dim=1):
    cos = cos[position_ids].unsqueeze(unsqueeze_dim)
    sin = sin[position_ids].unsqueeze(unsqueeze_dim)
    q_embed = (q * cos) + (rotate_half(q) * sin)
    k_embed = (k * cos) + (rotate_half(k) * sin)
    return q_embed, k_embed

class TaosPatchEmbedding(nn.Module):
    def __init__(self, config: TaosConfig):
        super().__init__()
        self.Taos_input_token_len = config.Taos_input_token_len
        self.Taos_emb = nn.Linear(config.Taos_input_token_len, config.Taos_hidden_size, bias=False)

    def forward(self, hidden_state: torch.Tensor):
        hidden_state = hidden_state.unfold(dimension=-1, size=self.Taos_input_token_len, step=self.Taos_input_token_len)
        return self.Taos_emb(hidden_state)

class TaosPointEmbedding(nn.Module):
    def __init__(self, config: TaosConfig):
        super().__init__()
        self.Taos_emb_layer = nn.Linear(config.Taos_input_token_len, config.Taos_hidden_size, bias=False)
        self.Taos_gate_layer = nn.Linear(config.Taos_input_token_len, config.Taos_hidden_size, bias=False)
        self.Taos_act_fn = ACT2FN[config.Taos_hidden_act]

    def forward(self, x):
        emb = self.Taos_act_fn(self.Taos_gate_layer(x)) * self.Taos_emb_layer(x)
        return emb

class TaosRotaryEmbedding(torch.nn.Module):
    def __init__(self, dim, max_position_embeddings=10000, base=10000, device=None):
        super().__init__()
        self.Taos_dim = dim
        self.Taos_max_position_embeddings = max_position_embeddings
        self.Taos_base = base
        inv_freq = 1.0 / (self.Taos_base ** (torch.arange(0, self.Taos_dim, 2, dtype=torch.int64).float().to(device) / self.Taos_dim))
        self.register_buffer("Taos_inv_freq", inv_freq, persistent=False)
        self._set_cos_sin_cache(seq_len=max_position_embeddings, device=self.Taos_inv_freq.device, dtype=torch.get_default_dtype())

    def _set_cos_sin_cache(self, seq_len, device, dtype):
        self.Taos_max_seq_len_cached = seq_len
        t = torch.arange(self.Taos_max_seq_len_cached, device=device, dtype=torch.int64).type_as(self.Taos_inv_freq)
        freqs = torch.outer(t, self.Taos_inv_freq)
        emb = torch.cat((freqs, freqs), dim=-1)
        self.register_buffer("Taos_cos_cached", emb.cos().to(dtype), persistent=False)
        self.register_buffer("Taos_sin_cached", emb.sin().to(dtype), persistent=False)

    def forward(self, x, seq_len=None):
        if seq_len > self.Taos_max_seq_len_cached:
            self._set_cos_sin_cache(seq_len=seq_len, device=x.device, dtype=x.dtype)
        return (self.Taos_cos_cached[:seq_len].to(dtype=x.dtype), self.Taos_sin_cached[:seq_len].to(dtype=x.dtype))

class TaosAttention(nn.Module):
    def __init__(self, config: TaosConfig, layer_idx: Optional[int] = None):
        super().__init__()
        self.Taos_layer_idx = layer_idx
        self.Taos_hidden_size = config.Taos_hidden_size
        self.Taos_num_heads = config.Taos_num_attention_heads
        self.Taos_head_dim = self.Taos_hidden_size // self.Taos_num_heads
        self.Taos_attention_dropout = config.Taos_attention_dropout
        self.Taos_q_proj = nn.Linear(self.Taos_hidden_size, self.Taos_hidden_size, bias=True)
        self.Taos_k_proj = nn.Linear(self.Taos_hidden_size, self.Taos_hidden_size, bias=True)
        self.Taos_v_proj = nn.Linear(self.Taos_hidden_size, self.Taos_hidden_size, bias=True)
        self.Taos_o_proj = nn.Linear(self.Taos_hidden_size, self.Taos_hidden_size, bias=False)
        self.Taos_rotary_emb = TaosRotaryEmbedding(self.Taos_head_dim, max_position_embeddings=config.Taos_max_position_embeddings)

    def forward(
            self,
            hidden_states: torch.Tensor,
            attention_mask: Optional[torch.Tensor] = None,
            position_ids: Optional[torch.LongTensor] = None,
            Taos_past_key_value: Optional[Cache] = None,
            output_attentions: bool = False,
            **kwargs,
    ) -> Tuple[torch.Tensor, Optional[torch.Tensor], Optional[Tuple[torch.Tensor]]]:
        bsz, q_len, _ = hidden_states.size()
        query_states = self.Taos_q_proj(hidden_states)
        key_states = self.Taos_k_proj(hidden_states)
        value_states = self.Taos_v_proj(hidden_states)
        query_states = query_states.view(bsz, q_len, self.Taos_num_heads, self.Taos_head_dim).transpose(1, 2)
        key_states = key_states.view(bsz, q_len, self.Taos_num_heads, self.Taos_head_dim).transpose(1, 2)
        value_states = value_states.view(bsz, q_len, self.Taos_num_heads, self.Taos_head_dim).transpose(1, 2)
        kv_seq_len = key_states.shape[-2]
        if Taos_past_key_value is not None:
            kv_seq_len += Taos_past_key_value.get_usable_length(kv_seq_len, self.Taos_layer_idx)
        cos, sin = self.Taos_rotary_emb(value_states, seq_len=kv_seq_len)
        query_states, key_states = apply_rotary_pos_emb(query_states, key_states, cos, sin, position_ids)
        if Taos_past_key_value is not None:
            key_states, value_states = Taos_past_key_value.update(key_states, value_states, self.Taos_layer_idx)
        attn_output = F.scaled_dot_product_attention(query_states, key_states, value_states, attention_mask, dropout_p=self.Taos_attention_dropout)
        attn_output = attn_output.transpose(1, 2).contiguous()
        attn_output = attn_output.reshape(bsz, q_len, self.Taos_hidden_size)
        attn_output = self.Taos_o_proj(attn_output)
        if not output_attentions:
            attn_weights = None
        return attn_output, attn_weights, Taos_past_key_value

class TaosMLP(nn.Module):
    def __init__(self, hidden_size: int, intermediate_size: int, hidden_act: str):
        super().__init__()
        self.Taos_hidden_size = hidden_size
        self.Taos_intermediate_size = intermediate_size
        self.Taos_gate_proj = nn.Linear(self.Taos_hidden_size, self.Taos_intermediate_size, bias=False)
        self.Taos_up_proj = nn.Linear(self.Taos_hidden_size, self.Taos_intermediate_size, bias=False)
        self.Taos_down_proj = nn.Linear(self.Taos_intermediate_size, self.Taos_hidden_size, bias=False)
        self.Taos_act_fn = ACT2FN[hidden_act]

    def forward(self, hidden_state):
        return self.Taos_down_proj(self.Taos_act_fn(self.Taos_gate_proj(hidden_state)) * self.Taos_up_proj(hidden_state))

class TaosDecoderLayer(nn.Module):
    def __init__(self, config: TaosConfig, layer_idx: int):
        super().__init__()
        self.Taos_self_attn = TaosAttention(config, layer_idx)
        self.Taos_ffn_layer = TaosMLP(hidden_size=config.Taos_hidden_size, intermediate_size=config.Taos_intermediate_size, hidden_act=config.Taos_hidden_act)
        self.Taos_norm1 = torch.nn.LayerNorm(config.Taos_hidden_size)
        self.Taos_norm2 = torch.nn.LayerNorm(config.Taos_hidden_size)

    def forward(
            self,
            hidden_states: torch.Tensor,
            attention_mask: Optional[torch.Tensor] = None,
            position_ids: Optional[torch.LongTensor] = None,
            Taos_past_key_value: Optional[Tuple[torch.Tensor]] = None,
            output_attentions: Optional[bool] = False,
            use_cache: Optional[bool] = False,
            **kwargs,
    ) -> Tuple[torch.FloatTensor, torch.FloatTensor, Optional[torch.FloatTensor], Optional[torch.FloatTensor]]:
        residual = hidden_states
        hidden_states, self_attn_weights, present_key_value = self.Taos_self_attn(
            hidden_states=hidden_states,
            attention_mask=attention_mask,
            position_ids=position_ids,
            Taos_past_key_value=Taos_past_key_value,
            output_attentions=output_attentions,
            use_cache=use_cache,
        )
        hidden_states = residual + hidden_states
        hidden_states = self.Taos_norm1(hidden_states)
        residual = hidden_states
        hidden_states = self.Taos_ffn_layer(hidden_states)
        hidden_states = residual + hidden_states
        hidden_states = self.Taos_norm2(hidden_states)
        if not output_attentions:
            self_attn_weights = None
        if not use_cache:
            present_key_value = None
        return hidden_states, self_attn_weights, present_key_value

class TaosPreTrainedModel(PreTrainedModel):
    config_class = TaosConfig
    base_model_prefix = "Taos_model"
    supports_gradient_checkpointing = True
    _no_split_modules = ["TaosDecoderLayer"]
    _skip_keys_device_placement = "Taos_past_key_values"
    _supports_flash_attn_2 = True
    _supports_sdpa = False
    _supports_cache_class = True

    def _init_weights(self, module):
        std = self.config.Taos_initializer_range
        if isinstance(module, torch.nn.Linear):
            module.weight.data.normal_(mean=0.0, std=std)
            if module.bias is not None:
                module.bias.data.zero_()
        elif isinstance(module, torch.nn.Embedding):
            module.weight.data.normal_(mean=0.0, std=std)
            if module.padding_idx is not None:
                module.weight.data[module.padding_idx].zero_()

class TaosModel(TaosPreTrainedModel):
    def __init__(self, config: TaosConfig):
        super().__init__(config)
        self.Taos_embed_layer = TaosPatchEmbedding(config)
        self.Taos_layers = nn.ModuleList([TaosDecoderLayer(config, layer_idx) for layer_idx in range(config.Taos_num_hidden_layers)])
        self.Taos_norm = torch.nn.LayerNorm(config.Taos_hidden_size)
        self.Taos_gradient_checkpointing = False

    def forward(
            self,
            input_ids: torch.FloatTensor = None,
            attention_mask: Optional[torch.Tensor] = None,
            position_ids: Optional[torch.LongTensor] = None,
            Taos_past_key_values: Optional[List[torch.FloatTensor]] = None,
            inputs_embeds: Optional[torch.FloatTensor] = None,
            use_cache: Optional[bool] = None,
            output_attentions: Optional[bool] = None,
            output_hidden_states: Optional[bool] = None,
            return_dict: Optional[bool] = None,
    ) -> Union[Tuple, MoeModelOutputWithPast]:
        output_attentions = output_attentions if output_attentions is not None else self.config.output_attentions
        output_hidden_states = output_hidden_states if output_hidden_states is not None else self.config.output_hidden_states
        use_cache = use_cache if use_cache is not None else self.config.Taos_use_cache
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        if input_ids is not None and inputs_embeds is not None:
            raise ValueError("You cannot specify both decoder_input_ids and decoder_inputs_embeds at the same time")
        elif input_ids is not None:
            batch_size, seq_length = input_ids.shape
        elif inputs_embeds is not None:
            batch_size, seq_length, _ = inputs_embeds.shape
        else:
            raise ValueError("You have to specify either decoder_input_ids or decoder_inputs_embeds")

        if inputs_embeds is None:
            inputs_embeds = self.Taos_embed_layer(input_ids)
            seq_length = inputs_embeds.shape[1]

        if self.Taos_gradient_checkpointing and self.training:
            if use_cache:
                use_cache = False

        past_key_values_length = 0
        if use_cache:
            use_legacy_cache = not isinstance(Taos_past_key_values, Cache)
            if use_legacy_cache:
                Taos_past_key_values = DynamicCache.from_legacy_cache(Taos_past_key_values)
            past_key_values_length = Taos_past_key_values.get_usable_length(seq_length)

        if position_ids is None:
            device = input_ids.device if input_ids is not None else inputs_embeds.device
            position_ids = torch.arange(past_key_values_length, seq_length + past_key_values_length, dtype=torch.long, device=device)
            position_ids = position_ids.view(-1, seq_length)
        else:
            position_ids = position_ids.view(-1, seq_length).long()

        attention_mask = _prepare_4d_causal_attention_mask(attention_mask, (batch_size, seq_length), inputs_embeds, past_key_values_length, sliding_window=None)
        hidden_states = inputs_embeds

        all_hidden_states = () if output_hidden_states else None
        all_self_attns = () if output_attentions else None
        next_decoder_cache = None

        for decoder_layer in self.Taos_layers:
            if output_hidden_states:
                all_hidden_states += (hidden_states,)
            if self.Taos_gradient_checkpointing and self.training:
                layer_outputs = self._gradient_checkpointing_func(
                    decoder_layer.__call__,
                    hidden_states,
                    attention_mask,
                    position_ids,
                    Taos_past_key_values,
                    output_attentions,
                    use_cache,
                )
            else:
                layer_outputs = decoder_layer(
                    hidden_states,
                    attention_mask=attention_mask,
                    position_ids=position_ids,
                    Taos_past_key_value=Taos_past_key_values,
                    output_attentions=output_attentions,
                    use_cache=use_cache,
                )
            hidden_states = layer_outputs[0]
            if output_attentions:
                all_self_attns += (layer_outputs[1],)
            if use_cache:
                next_decoder_cache = layer_outputs[2]

        hidden_states = self.Taos_norm(hidden_states)
        if output_hidden_states:
            all_hidden_states += (hidden_states,)

        next_cache = None
        if use_cache:
            next_cache = next_decoder_cache.to_legacy_cache() if use_legacy_cache else next_decoder_cache

        if not return_dict:
            return tuple(v for v in [hidden_states, next_cache, all_hidden_states, all_self_attns] if v is not None)
        return MoeModelOutputWithPast(
            last_hidden_state=hidden_states,
            past_key_values=next_cache,
            hidden_states=all_hidden_states,
            attentions=all_self_attns,
        )

class TaosForPrediction(TaosPreTrainedModel, TaosTSGenerationMixin):
    def __init__(self, config: TaosConfig):
        super().__init__(config)
        self.config = config
        self.Taos_model = TaosModel(self.config)
        lm_head_list = []
        self.Taos_output_token_len_map = {}
        for i, output_token_len in enumerate(self.config.Taos_output_token_lens):
            lm_head_list.append(nn.Linear(self.config.Taos_hidden_size, output_token_len, bias=False))
            self.Taos_output_token_len_map[output_token_len] = i
        self.Taos_lm_heads = nn.ModuleList(lm_head_list)
        self.Taos_loss_function = torch.nn.MSELoss(reduction='none')
        self.post_init()

    def set_decoder(self, decoder):
        self.Taos_model = decoder

    def get_decoder(self):
        return self.Taos_model

    def forward(
            self,
            input_ids: torch.FloatTensor = None,
            attention_mask: Optional[torch.Tensor] = None,
            position_ids: Optional[torch.LongTensor] = None,
            Taos_past_key_values: Optional[List[torch.FloatTensor]] = None,
            inputs_embeds: Optional[torch.FloatTensor] = None,
            labels: Optional[torch.FloatTensor] = None,
            loss_masks: Optional[torch.FloatTensor] = None,
            use_cache: Optional[bool] = None,
            output_attentions: Optional[bool] = None,
            output_hidden_states: Optional[bool] = None,
            return_dict: Optional[bool] = None,
            max_output_length: Optional[int] = None,
            revin: Optional[bool] = False,
    ) -> Union[Tuple, MoeCausalLMOutputWithPast]:
        output_attentions = output_attentions if output_attentions is not None else self.config.output_attentions
        output_hidden_states = output_hidden_states if output_hidden_states is not None else self.config.output_hidden_states
        return_dict = return_dict if return_dict is not None else self.config.use_return_dict

        if revin:
            mean, std = input_ids.mean(dim=-1, keepdim=True), input_ids.std(dim=-1, keepdim=True)
            input_ids = (input_ids - mean) / std
        outputs = self.Taos_model(
            input_ids=input_ids,
            attention_mask=attention_mask,
            position_ids=position_ids,
            Taos_past_key_values=Taos_past_key_values,
            inputs_embeds=inputs_embeds,
            use_cache=use_cache,
            output_attentions=output_attentions,
            output_hidden_states=output_hidden_states,
            return_dict=return_dict,
        )
        hidden_states = outputs[0] if not return_dict else outputs.last_hidden_state
        predictions = None
        loss = None
        if labels is not None:
            ar_loss = 0.0
            for lm_head, output_token_len in zip(self.Taos_lm_heads, self.config.Taos_output_token_lens):
                one_predictions = lm_head(hidden_states)
                one_loss = self.calc_ar_loss(one_predictions, labels, loss_masks, output_token_len)
                ar_loss += one_loss
                if predictions is None:
                    predictions = one_predictions
            loss = ar_loss / len(self.config.Taos_output_token_lens)
        else:
            if max_output_length is None:
                output_token_len = self.config.Taos_output_token_lens[0]
                max_output_length = output_token_len
            else:
                output_token_len = self.config.Taos_output_token_lens[0]
                for h in self.config.Taos_output_token_lens[1:]:
                    if h > max_output_length:
                        break
                    else:
                        output_token_len = h
            lm_head = self.Taos_lm_heads[self.Taos_output_token_len_map[output_token_len]]
            predictions = lm_head(hidden_states)[:, -1, :]
            if output_token_len > max_output_length:
                predictions = predictions[:, :max_output_length]
            if revin:
                predictions = predictions * std + mean
        if not return_dict:
            output = (predictions,) + outputs[1:]
            return (loss) + output if loss is not None else output
        return MoeCausalLMOutputWithPast(
            loss=loss,
            logits=predictions,
            past_key_values=outputs.past_key_values,
            hidden_states=outputs.hidden_states,
            attentions=outputs.attentions,
        )

    def calc_ar_loss(self, predictions, labels, loss_masks, output_token_len):
        seq_len = predictions.shape[1] * self.config.Taos_input_token_len
        labels = labels[:, :seq_len - self.config.Taos_input_token_len + output_token_len]
        shift_labels = labels.unfold(dimension=-1, size=output_token_len, step=self.config.Taos_input_token_len)
        losses = self.Taos_loss_function(predictions, shift_labels).mean(dim=-1)
        if loss_masks is not None:
            losses = losses * loss_masks
            loss = losses.sum() / loss_masks.sum()
        else:
            loss = torch.mean(losses)
        return loss

    def prepare_inputs_for_generation(
            self, input_ids, Taos_past_key_values=None, attention_mask=None, inputs_embeds=None, revin=True, **kwargs
    ):
        if Taos_past_key_values is not None:
            if isinstance(Taos_past_key_values, Cache):
                cache_length = Taos_past_key_values.get_seq_length()
                if isinstance(Taos_past_key_values, DynamicCache):
                    past_length = Taos_past_key_values.seen_tokens
                else:
                    past_length = cache_length
                max_cache_length = Taos_past_key_values.get_max_length()
            else:
                cache_length = past_length = Taos_past_key_values[0][0].shape[2]
                max_cache_length = None
            if attention_mask is not None and attention_mask.shape[1] > (input_ids.shape[1] // self.config.Taos_input_token_len):
                input_ids = input_ids[:, -(attention_mask.shape[1] - past_length):]
            elif past_length < (input_ids.shape[1] // self.config.Taos_input_token_len):
                input_ids = input_ids[:, past_length * self.config.Taos_input_token_len:]
            if max_cache_length is not None and attention_mask is not None and cache_length + (input_ids.shape[1] // self.config.Taos_input_token_len) > max_cache_length:
                attention_mask = attention_mask[:, -max_cache_length:]

        position_ids = kwargs.get("position_ids", None)
        if attention_mask is not None and position_ids is None:
            position_ids = attention_mask.long().cumsum(-1) - 1
            position_ids.masked_fill_(attention_mask == 0, 1)
            if Taos_past_key_values:
                position_ids = position_ids[:, -(input_ids.shape[1] // self.config.Taos_input_token_len):]

        if inputs_embeds is not None and Taos_past_key_values is None:
            model_inputs = {"inputs_embeds": inputs_embeds}
        else:
            model_inputs = {"input_ids": input_ids}

        model_inputs.update({
            "position_ids": position_ids,
            "Taos_past_key_values": Taos_past_key_values,
            "use_cache": kwargs.get("use_cache"),
            "attention_mask": attention_mask,
            "revin": revin
        })
        return model_inputs


def init_model():
    global Taos_model

    config = TaosConfig(
        Taos_input_token_len=96,
        Taos_hidden_size=1024,
        Taos_num_attention_heads=8,
        Taos_intermediate_size=2048,
        Taos_num_hidden_layers=8,
        Taos_max_position_embeddings=2048,
        Taos_attention_dropout=0.1,
        Taos_hidden_act="gelu",
        Taos_output_token_lens=[96],
    )

    Taos_model = TaosForPrediction(config)
    weight_path = "/var/lib/taos/taosanode/model/tdtsfm/taos.pth"
    state_dict = torch.load(weight_path, map_location=torch.device('cpu'))

    # convert model weight
    Taos_model.load_state_dict(state_dict, strict=True)
    Taos_model = Taos_model.to(device)

@app.route('/tdtsfm', methods=['POST'])
def ds_predict():
    print(f"start predict")

    """处理POST请求并返回模型预测结果"""
    try:
        # 获取POST请求中的JSON数据
        data = request.get_json()
        if not data or 'input' not in data:
            return jsonify({
                'status':'error',
                'error': 'Invalid input, please provide "input" field in JSON'
            }), 400

        print(f"data:{data}")

        input_data = data['input']
        num_len = data['next_len']

        if len(set(input_data)) == 1:
            # for identical array list, std is 0, return directly
            pred_y = [input_data[0] for _ in range(num_len)]
        else:
            if Taos_model is None:
                return jsonify({
                    'error': f'not predict, load model failed'
                }), 500

            seqs = torch.tensor(input_data).unsqueeze(0).float().to(device)

            # 禁用梯度并生成预测
            with torch.no_grad():
                Taos_model.eval()
                pred_y = Taos_model.generate(seqs, max_new_tokens=num_len)

            print(f"pred result is:({pred_y})")
            pred_y = pred_y[0].tolist()
        response = {
            'status': 'success',
            'output': pred_y[-num_len:]
        }
        return jsonify(response), 200
    except Exception as e:
        print(f"error when predict:{e}")
        return jsonify({
            'error': f'Prediction failed: {str(e)}'
        }), 500


def main():
    init_model()

    app.run(
        host='0.0.0.0',
        port=6036,
        threaded=True,
        debug=False
    )


if __name__ == "__main__":
    main()
