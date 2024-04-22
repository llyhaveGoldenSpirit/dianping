package com.lly.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
* 定义在处理用户请求过程中的拦截器方法，校验用户的拦截器，在MvcConfig里面配置
* 拦截器是在spring容器初始化haul之前执行的，加什么Component注解都没有
* 这个对象是我们手动创建的，所以不能自动装配，只能手动注入
* */
public class LoginInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        /*// 1.获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)) {
            //不存在，拦截，返回401状态码
            response.setStatus(401);
            return false;
        }
        // 2.基于token获取redis中的用户
        String key = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
        //3.判断用户是否存在
        if (userMap.isEmpty()){
            //4.不存在，拦截，返回401
            response.setStatus(401);
            return false;
        }
        //5.将查询到的Hash数据转为UserDTO对象
        UserDTO user = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
        // 6.存在，保存用户信息到ThreadLocal
        UserHolder.saveUser((UserDTO)user);

        // 7.刷新token有效期
        stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);*/

        //1.判断是否需要拦截（ThreadLocal中是否有用户
        if (UserHolder.getUser()==null){
            //没有，需要拦截，设置状态码
            response.setStatus(401);
            return false;
        }
        //8.放行
        return true;
    }

}
