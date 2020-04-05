package com.hugh.back.service;

import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.hugh.back.dao.DictDao;
import com.hugh.common.sys.entity.DictEntity;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.HashOperations;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

@Service
@Slf4j
public class RedisHashService extends ServiceImpl<DictDao, DictEntity> {

    private static final String RedisHashKey = "Redis:Hash";

    @Autowired
    private DictDao dictDao;

    @Autowired
    private RedisTemplate redisTemplate;


    /**
     * 添加数据字典及其对应的选项(code-value)
     *
     * @param config
     * @return
     * @throws Exception
     */
    @Transactional(rollbackFor = Exception.class)
    public String addSysDictConfig(DictEntity config) throws Exception {
//        int res = dictDao.insert(config);
//        if (res > 0) {
        //实时触发数据字典的hash存储
        cacheConfigMap(config);
//        }
        return config.getId();
    }


    /**
     * 取出缓存中所有的数据字典列表
     */
    public Map<String, List<DictEntity>> getSysDictConfig() throws Exception {
        return getAllCacheConfig();
    }


    /**
     * 取出缓存中特定的数据字典列表
     *
     * @param type
     * @return
     * @throws Exception
     */
    public List<DictEntity> getByType(final String type) throws Exception {
        return getCacheConfigByType(type);
    }


    /**
     * 实时获取所有有效的数据字典列表-转化为map-存入hash缓存中
     */
    @Async
    public void cacheConfigMap(DictEntity config) {
        try {
            List<DictEntity> sysDictConfigList = dictDao.getAll(config);
            if (sysDictConfigList != null && !sysDictConfigList.isEmpty()) {
                Map<String, List<DictEntity>> dataMap = Maps.newHashMap();
                //所有的数据字典列表遍历 -> 转化为 hash存储的map
                sysDictConfigList.forEach(sysDictConfig -> {
                    List<DictEntity> list = dataMap.get(sysDictConfig.getType());
                    if (CollectionUtils.isEmpty(list)) {
                        list = Lists.newLinkedList();
                    }
                    list.add(sysDictConfig);
                    dataMap.put(sysDictConfig.getType(), list);
                });
                //存储到缓存hash中
                HashOperations<String, String, List<DictEntity>> hashOperations = redisTemplate.opsForHash();
                hashOperations.putAll(RedisHashKey, dataMap);
            }
        } catch (Exception e) {
            log.error("实时获取所有有效的数据字典列表-转化为map-存入hash缓存中-发生异常：", e.fillInStackTrace());
        }
    }


    /**
     * 从缓存hash中获取所有的数据字典配置map
     *
     * @return
     */
    public Map<String, List<DictEntity>> getAllCacheConfig() {
        Map<String, List<DictEntity>> map = Maps.newHashMap();
        try {
            HashOperations<String, String, List<DictEntity>> hashOperations = redisTemplate.opsForHash();
            map = hashOperations.entries(RedisHashKey);
        } catch (Exception e) {
            log.error("从缓存hash中获取所有的数据字典配置map-发生异常：", e.fillInStackTrace());
        }
        return map;
    }


    /**
     * 从缓存hash中获取特定的数据字典列表
     *
     * @param type
     * @return
     */
    public List<DictEntity> getCacheConfigByType(final String type) {
        List<DictEntity> list = Lists.newArrayList();
        try {
            HashOperations<String, String, List<DictEntity>> hashOperations = redisTemplate.opsForHash();
            list = hashOperations.get(RedisHashKey, type);
        } catch (Exception e) {
            log.error("从缓存hash中获取特定的数据字典列表-发生异常：", e.fillInStackTrace());
        }
        return list;
    }

}
