#
本项目是在线监听redis wait_queue队列,有数据就解析json 字符串,上传解析后的文件到fastdfs,返回fileid 保存到键为 new_files的redis 队列。