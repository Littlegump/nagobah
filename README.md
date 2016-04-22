# Nagobah

标签（空格分隔）： dagobah

---

##[What is Nagobah][1]
--it's for dagobah

##[But what is Dagobah?][2]
>. Con_scheduler with `task dependencies`
>. Independent `schduler` with `cron syntax`
>. Get the `STDOUT`,`STDERR`,`RETURN`
>. Name A `Remote_user` AT a `Remote_host` to exec the command
>. `MongoDB` & `SQLite` supported
>. `Web UI` to monitor

## Then what the Nagobah can do?
It's just a scripts to deploy dagobah_job in batch mode. What is means? Just look at the pic.

```
pip install nagobah
nagobah --help
```

you have 7 tasks to run, 1-6 will be deploy to hunderds of host. And Only if all of these 1-6 tasks successfully executed that the task **7** can week up.

## Quick picture start
<font color="nattiergreen">**(淡青色)nattiergreen**</font> means it can host at `ONE host`
<font color="orange">**(橘黄色)orange**</font> means it will be deployed at the `hosts you defined`
Your module may looks like this `1->2-3->4->5->6->7`
The Totally Job `What you WANT` might be 
**<font color="red">this</font>**:
![图片1.png-37.1kB][3]
**<font color="red">Or this</font>**
![微信截图_20160422150623.png-46.9kB][4]
![微信截图_20160422151331.png-61.4kB][5]

**<font color="red">These all you can imagine</font>**...
Just Read the nagobah docs for more help



The totally job what you want IS


  [1]: https://github.com/littlegump/nagobah
  [2]: https://github.com/thieman/dagobah
  [3]: http://static.zybuluo.com/littlegump/r3djdbka81a3zbvlt5d2grbj/%E5%9B%BE%E7%89%871.png
  [4]: http://static.zybuluo.com/littlegump/0v3wstx8gn9u0d51m3md23re/%E5%BE%AE%E4%BF%A1%E6%88%AA%E5%9B%BE_20160422150623.png
  [5]: http://static.zybuluo.com/littlegump/wpi5i30skx46k367rsd8v5b2/%E5%BE%AE%E4%BF%A1%E6%88%AA%E5%9B%BE_20160422151331.png
