# PRIME Training

We implemented our reinforcement learning algorithm extending from [veRL](https://github.com/volcengine/verl). 

## Installation 
Please refer to the [veRL documentation](https://verl.readthedocs.io/en/latest/start/install.html) to solve dependencies. Only FSDP backend support is required to run our code. 

## Start Training
We provide an example bash script to launch the training task. The prompt data can be downloaded [here](https://huggingface.co/datasets/PRIME-RL/Eurus-2-RL-Data). Please remember to modify the paths in the script. 

It is needed to change the directories in `examples/run_prime_main.sh`

```bash
PROJECT_NAME='PRIME'
EXPERIMENT_NAME='online-after-solvable-0.2-0.8-policy-self-ref'
DATA_PATH=path/to/data
SFT_MODEL_PATH=PRIME-RL/Eurus-2-7B-SFT
CKPT_PATH=path/to/save/dir
```
Then you can run
```bash
bash examples/run_prime_main.sh
```

## Config Explanation
We made several vital extensions to the original training pipeline of the PPO algorithm. Here we provide explanation to configure these new features. 

Please refer to [this page](https://verl.readthedocs.io/en/latest/examples/config.html) for a thorough guide of other basic training settings. 

### Prompt filtering
During the rollout stage, we find that choosing appropriate prompts matters a lot, especially only preserving the prompts among a certain difficulty range. Inspired by [Qwen-2.5-Math](https://arxiv.org/abs/2409.12122), which filtered prompts according to the accuracy of the initial policy model beforehand, we perform online prompt filtering throughout the training. We sample multiple trajectories for each prompt, then calculate the accuracy and preserve the prompts with accuracy scores within a certain range. This also balanced the training data distribution for PRM update. 

```yaml
data:
  n_samples: 4 
  filter_accuracy: True
  filter_truncated: True
  accuracy_lower_bound: 0.2
  accuracy_upper_bound: 0.8
  oversample_factor: 1
  system_prompt: null
```
- ``data.n_samples``: Amount of trajectories for each prompt.
- ``data.filter_accuracy``: Whether to enable prompt filtering. 
- ``data.filter_truncated``: Whether to filter out truncated reponses that reach max length.
- ``data.accuracy_lower_bound`` ``data.accuracy_upper_bound``: The range of accuracy to preserve the prompt.
- ``data.oversample_factor``: Oversample factor for the filtered prompts. Will sample ``batch_size * oversample_factor`` prompts for each batch.
- ``data.system_prompt``: System prompt to use. If set to ``null``, will use the default system prompt in dataset.


### Implicit Process Reward
We adopt [implicit PRM](https://arxiv.org/abs/2412.01981) which obtains dense rewards by training on the cheaper response-level labels. To train the policy model, we combine the process rewards and the ground truth outcome rewards to calculate the advantage. During reinforcement learning, the PRM is updated according to the ground truth label. 

The reference model in implicit PRM can be set as either the initial SFT model or the up-to-date policy model. Both strategies yield similar performance. 

In this work, we update the implicit PRM with cross entropy(CE) loss due to memory efficiency.
```yaml
reward_model:
  rm_coef: 5
  rm_type: prime
  prime_granularity: token
  prime_norm: batch_norm
  mini_batch_size: 256
  micro_batch_size: 64
  prime_model:
    path: PRIME-RL/Eurus-2-7B-SFT
    ref_type: freeze
    ref_path: PRIME-RL/Eurus-2-7B-SFT
    update: after
    beta_train: 0.05
    loss_type: ce
    use_remove_padding: False
```
- ``reward_model.rm_coef``: Weight for reward model in the reward combination. 
- ``reward_model.rm_type``: Type of reward model, ``prime`` for implicit PRM and ``value`` for normal reward models. 
- ``reward_model.prime_granularity``: Granularity to assign reward value. If set to ``token``, every token will have a process reward. If set to ``whole``, the reward will only appear on the last token just like vanilla RLHF. 
- ``reward_model.prime_norm``: How to normalize process reward value to stabilize training. Default to ``batch_norm``
- ``reward_model.mini_batch_size``: Mini batch size for PRM update. **Should be divisible by world_size.**
- ``reward_model.micro_batch_size``: Micro batch size for PRM update. **Should be divisible by world_size.**
- ``reward_model.path``: Model as initialization of implicit PRM. 
- ``reward_model.ref_type``: PRM Reference model type. ``freeze``: the reference model has frozen parameters. ``policy``: the reference model synchronizes with the policy model. 
- ``reward_model.ref_path``: Reference model in implicit PRM. 
- ``reward_model.prime_model.update``: ``none`` to disable online PRM update, ``after`` to update the PRM after the policy model (Single-Forward), ``before`` to update the PRM before the policy model (Double-Forward). 
- ``reward_model.prime_model.beta_train``: Beta value used to update the PRM. 
- ``reward_model.prime_model.loss_type``: Loss function to update the PRM. Only ``ce`` is supported now.
- ``reward_model.prime_model.use_remove_padding``: Whether to remove padding tokens in the PRM loss calculation. Turn on to accelerate and save memory. Support Llama, Qwen, Mistral, and InternLM for now.

### Advantage Estimation
From pilot study, we compared different online RL algorithms and found that REINFORCE-like algorithms, despite simpler than PPO, are strong enough to produce stable results. We choose the best performing [RLOO](https://arxiv.org/abs/2402.14740) as our RL algorithm. 

To extend RLOO to support process rewards and merging different reward types (ground truth and reward model), we perform Leave-One-Out separately on different rewards. We directly use the return value of RLOO as advantage. 
```yaml
algorithm:
  adv_estimator: rloo
  adv_params:
    verifier_gamma: ${algorithm.gamma}
    reward_model_gamma: ${algorithm.gamma}
```
- ``algorithm.adv_estimator``: The advantage estimator, like ``gae``,``rloo``. If the advantage does not need a value estimation model, the critic model is completely disabled during training. 
- ``algorithm.adv_params.verifier_gamma`` ``algorithm.adv_params.reward_model_gamma``: Set separate gamma for different sources of reward

## TODO
- [ ] Support saving PRM model during training.
- [ ] Support for other RL algorithms, REINFORCE, PPO, GRPO, ReMax, etc.
- [ ] Support LoRA.
