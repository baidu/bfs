# 怎样成为贡献者
1. 首先fork本项目到你的个人目录, 尝试修改你发现的问题
2. 代码风格请遵守Google编码规范, 但使用4空格缩进
3. 如果代码里要打日志, 请遵守[BFS日志规范](log_rule.md)
4. 开发完成后自测功能是否正确，并运行`make test`及`make check`检查是否可以通过已有的测试case
5. 提交代码时的commit message 请遵守:  
    a. 首行使用英文描述 用简短的句子描述你做出的更改, 首字母大写.  
    b. 第二行空行, 第三行写(#对应的issue号).  
    c. 后面用英文详细描述你的升级的细节, 如果首行已经描述清楚这里也可以省略.  
    示例如下:  
    Add RebuildBlock in BlockMapping  

    (#617) 'AddNewBlock' is used both in adding new blocks and rebuild  
    exist blocks. Use separate functions to deal with different  
    situations.  
6. 提交pull request  
7. 在code-review通过后，你的代码便有机会运行在百度的数万台服务器上~  
