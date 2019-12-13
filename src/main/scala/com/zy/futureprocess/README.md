特征提取、转换和选择  
1、特征提取是指从原始数据中提取特征。  
TF-IDF、word2Vec  
TF-IDF:用分解器Tokenizer把句子划为单个词语。对每一个句子（词袋），用  
hashingTF将句子转为特征向量。  
最后IDF重新调整特征向量。    