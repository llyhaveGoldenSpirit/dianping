package com.lly.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.lly.dto.Result;
import com.lly.entity.VoucherOrder;
import com.lly.mapper.VoucherOrderMapper;
import com.lly.service.ISeckillVoucherService;
import com.lly.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.lly.utils.CacheClient;
import com.lly.utils.RedisIdWorker;
import com.lly.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    RedisIdWorker redisIdWorker;

    @Resource
    private CacheClient cacheClient;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    //静态代码块只会执行一次，不会浪费IO资源
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    //线程池(实际只有一个线程）
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    String queueName = "stream.orders";

    @PostConstruct//这个注解啥意思
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{


        @Override
        public void run() {
            while (true){
                try {
                    //1.获取消息队列中的订单信息XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    //2.判断消息获取是否成功
                    if (list==null||list.isEmpty()){
                        //2.1 如果获取失败，说明没有消息，继续下一次循环
                        continue;
                    }

                    //3.解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.创建订单
                    handleVoucherOrder(voucherOrder);
                    //5. ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                } catch (Exception e) {
                    log.error("处理订单异常",e);
                    handlePendingList();
                }

            }
        }
    }

    private void handlePendingList() {
        while (true){
            try {
                //1.获取pending-list中的订单信息XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS streams.orders 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from("g1", "c1"),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(queueName, ReadOffset.from("0"))
                );

                //2.判断消息获取是否成功
                if (list==null||list.isEmpty()){
                    //2.1 如果获取失败，说明pending-list没有异常消息，继续下一次循环
                    break;
                }

                //3.解析消息中的订单信息
                MapRecord<String, Object, Object> record = list.get(0);
                Map<Object, Object> values = record.getValue();
                VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                //4.创建订单
                handleVoucherOrder(voucherOrder);
                //5. ACK确认 SACK stream.orders g1 id
                stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

            } catch (Exception e) {
                log.error("处理pending-list订单异常",e);
                try {
                    Thread.sleep(20);
                } catch (InterruptedException ex) {
                    throw new RuntimeException(ex);
                }
            }

        }
    }
    /*
    使用redis的Stream后不需要阻塞队列了
    //阻塞队列
    private BlockingQueue<VoucherOrder> orderTasts = new ArrayBlockingQueue<>(1024*1024);
    //线程任务 内部类
    private class VoucherOrderHandler implements Runnable{

        @Override
        public void run() {
            while (true){
                try {
                    //1.获取队列中的订单信息
                    VoucherOrder voucherOrder = orderTasts.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常",e);
                }

            }
        }
    }*/

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        //现在这个线程是从线程池获得的一个全新的线程，所以不能从UserHolser获取userId了
        //1.所以我们只能从voucherOrder中去取用户id
        Long userId = voucherOrder.getUserId();
        //2.创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //3.获取锁
        boolean isLOck = lock.tryLock();
        //4.判断是否获取锁成功
        if (!isLOck){
            //获取锁失败，返回错误或重试
            //return Result.fail("不允许重复下单");
            log.error("不允许重复下单");
            return;
        }
        try{
            /*//获取代理对象（事务）
            现在这里是拿不到的，因为这个底层是通过线程去获取，当前线程是一个全新子线程
            所以我们要提前在主线程获取
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();*/
            proxy.createVoucherOrder(voucherOrder);
        }finally {
            lock.unlock();
        }
    }

    //为了让子线程拿到，放到成员变量
    IVoucherOrderService proxy;


    /*
   * 基于Redis完成秒杀资格判断
   * */

    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //获取订单id
        long orderId = redisIdWorker.nextId("order");
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );

        //2.判断结果是否为0
        int r = result.intValue();
        if (r!=0){
            //2.1 不为0，没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }


        //3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //异步下单，开启独立线程，不断执行线程任务

        //4.返回订单id
        return Result.ok(orderId);

    }
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();
        //1.执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );

        //2.判断结果是否为0
        int r = result.intValue();
        if (r!=0){
            //2.1 不为0，没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
        }

        //2.2 为0，有购买资格，把下单信息保存到阻塞队列

        //TODO 保存 阻塞队列
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3 订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.4用户id

        voucherOrder.setUserId(userId);
        //2.5 代金卷id
        voucherOrder.setVoucherId(voucherId);
        //放入阻塞队列
        orderTasts.add(voucherOrder);

        //3.获取代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //异步下单，开启独立线程，不断执行线程任务

        //4.返回订单id
        return Result.ok(orderId);

    }*/


    /*@Override
    使用java代码实现查数据库实现秒杀逻辑
    public Result seckillVoucher(Long voucherId) {
        //1.查询优惠卷
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //2、判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())){
            //尚未开始
            return Result.fail("活动还没有开始");
        }

        //3、判断秒杀是否结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())){
            //秒杀已经结束
            return Result.fail("活动已结束");
        }

        //4、判断库存是否充足
        if (voucher.getStock()<1){
            //库存不足
            return Result.fail("优惠价已经抢完了");
        }

        //6、一人一单
        Long userId = UserHolder.getUser().getId();
        *//*
         * 给用户加锁，每个用户加一把锁
         * .intern()牛逼
         * 如果这个锁加在方法内部的话，锁里面的操作完成了
         * 别的线程就可以进来继续操作，如果此时事务还没有提交，就又会出现类似库存超卖的问题
         * 所以锁要加在函数外面
         * *//*
        *//*synchronized(userId.toString().intern()) {
            //7、返回订单id
           *//**//*
            return createVoucherOrder(voucherId);
            直接这样相当于this.createVoucherOrder(voucherId)
            是调用的本对象的方法，而非代理对象，这样事务就会失效
            应该先获取代理对象（事务），再调用这个方法
            注意这个方法，接口里因该有
           * *//**//*
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }*//*
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLOck = lock.tryLock();
        //判断是否获取锁成功
        if (!isLOck){
            //获取锁失败，返回错误或重试
            return Result.fail("不允许重复下单");
        }
        try{
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        }finally {
            lock.unlock();
        }

    }
*/
    /*
    * 创建订单添加而非修改，只能用悲观锁
    * spring拿到当前对象的代理对象，然后拿的代理对象来做事务处理
    * */
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder){
        //6、一人一单
        Long userId = voucherOrder.getUserId();
        //6、1 查询订单
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        //6.2 判断是否存在
        if (count>0){
            //用户已经购买过了
            //return Result.fail("用户已经购买过一次了！");
            log.error("用户已经购买过一次了！");
            return;
        }

        //5、扣减库存，这里怎么扣减，连数据库扣减吗
        //voucher.setStock(voucher.getStock()-1);
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id",voucherOrder.getVoucherId()).gt("stock",0)//where id = ？ and stock > 0;
                .update();
        if (!success){
            //扣减失败
            //return Result.fail("优惠价已经抢完了");
            log.error("库存不足！");
            return;
        }

        /*//6、创建订单，订单的信息有哪些啊，怎么创建
        VoucherOrder voucherOrder = new VoucherOrder();
        //6.1 订单id
        Long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //6.2 用户id

        voucherOrder.setUserId(userId);
        //6.3 代金卷id
        voucherOrder.setVoucherId(voucherId);*/
        save(voucherOrder);
        //return Result.ok(orderId);
    }
}
