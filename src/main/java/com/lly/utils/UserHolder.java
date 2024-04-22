package com.lly.utils;

import com.lly.dto.UserDTO;

/*
* 用于在多线程环境下存储和获取用户信息
* */
public class UserHolder {
    //线程局部变量，通过它存储和获取特定于该线程的数据
    private static final ThreadLocal<UserDTO> tl = new ThreadLocal<>();

    //将用户信息存储到线程变量中，在多线程环境下，每个用户存储的用户信息都是相互隔离的
    //不会相互干扰
    public static void saveUser(UserDTO user){
        tl.set(user);
    }

    //从线程变量中获取当前线程对应的用户信息
    public static UserDTO getUser(){
        return tl.get();
    }

    //清除当前线程对应的用户信息，防止内存泄露
    public static void removeUser(){
        tl.remove();
    }
}
