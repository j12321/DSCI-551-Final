# DSCI-551-Final

## Things to do before running the code
1. Install huggingface_hub
2. Get your hugging face interface api at https://huggingface.co/settings/tokens

   create new token -> token type: read
3. Replace your token at huggingface.py line 12


## Instructions to run hf_deepseek.py
1. Get your hugging face token
2. Create Inference Endpoint at https://endpoints.huggingface.co/new?repository=deepseek-ai/DeepSeek-R1-Distill-Qwen-32B
3. Get the Endpoint Url after starting the endpoint
4. Replace your hugging face token at line 13
5. Replace your Endpoint Url at line 14

!! Make sure to stop the endpoint while not using. The charge rate is $3.8/ hr
