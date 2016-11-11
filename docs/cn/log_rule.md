## 日志规则与说明
BFS中日志统一使用common库中LOG宏打印，printf风格。另外为了便于grep, 制订以下规则：  
1. 所有block id的打印使用“#%ld "的格式（即前加#，后加空格）  
2. 所有chunkserver id打印使用"C%d "的格式  
3. 所有entry id打印使用"E%ld "的格式  
4. 所有block version打印使用"V%ld "的格式  
