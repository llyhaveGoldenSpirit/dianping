package com.lly.utils;

import lombok.Data;

import java.time.LocalDateTime;

/*
* 要使用逻辑过期的方法解决缓存击穿问题，需要添加有效时间这一属性
* 不能再shop原有类上加，这样修改原有代码的编码方式是不好的
* 所以另外写一个类，组合时间和热key对象
* */
@Data
public class RedisData {
    private LocalDateTime expireTime;
    private Object data;
}
