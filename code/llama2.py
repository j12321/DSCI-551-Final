pip install torch transformers pymongo
from transformers import AutoTokenizer, AutoModelForCausalLM
import torch

# your llama 2 model directory
model_path = "~/.llama/checkpoints/llama-2-7b-chat"

tokenizer = AutoTokenizer.from_pretrained(model_path)
model = AutoModelForCausalLM.from_pretrained(model_path,
                                             torch_dtype=torch.float16,
                                             device_map="auto")

def sql_query(natural_language):
    prompt = f'Convert this request into a SQL query:\n"{natural_language}"\nSQL:'
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        inputs.input_ids,
        max_new_tokens=100,
        temperature=0.2,
        do_sample=True,
        eos_token_id=tokenizer.eos_token_id
    )
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    if "SQL:" in result:
        return result.split("SQL:")[-1].strip()
    return result.strip()

def mongodb_query(natural_language):
    prompt = f'Convert this request into a MongoDB query:\n"{natural_language}"\nMongoDB:'
    inputs = tokenizer(prompt, return_tensors="pt").to(model.device)
    outputs = model.generate(
        inputs.input_ids,
        max_new_tokens=100,
        temperature=0.2,
        do_sample=True,
        eos_token_id=tokenizer.eos_token_id
    )
    result = tokenizer.decode(outputs[0], skip_special_tokens=True)
    if "MongoDB:" in result:
        return result.split("MongoDB:")[-1].strip()
    return result.strip()

# MySQL
sql_q = sql_query("Find the names of all employees who earn more than $50,000")
sql_q = sql_q.strip('\n')
print(sql_q)

# MongoDB
mongo_q = mongodb_query("Find the names of all employees who earn more than $50,000")
print(mongo_q)