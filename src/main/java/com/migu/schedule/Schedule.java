package com.migu.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.Task;
import com.migu.schedule.info.TaskInfo;

/*
*类名和方法不能修改
 */
public class Schedule
{
    // key为nodeId,1个服务器节点可以有多个不同的Task,由addTask()保证其不同
    private static Map<Integer, List<Task>> map =
        Collections.synchronizedMap(new HashMap<Integer, List<Task>>());
    
    // 挂起队列
    private static List<Task> hangUpList = Collections.synchronizedList(new ArrayList<Task>());
    
    public int init()
    {
        hangUpList.clear();
        map.clear();
        return ReturnCodeKeys.E001;
    }
    
    public int registerNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        
        if (map.containsKey(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        
        map.put(nodeId, null);
        return ReturnCodeKeys.E003;
    }
    
    public int unregisterNode(int nodeId)
    {
        if (nodeId <= 0)
        {
            return ReturnCodeKeys.E004;
        }
        
        if (!map.containsKey(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        // 如果该服务节点正运行任务
        List<Task> list = map.get(nodeId);
        if (null != list && list.size() > 0)
        {
            // 将运行的任务移到任务挂起队列中，等待调度程序调度。
            hangUpList.addAll(list);
        }
        map.remove(nodeId);
        return ReturnCodeKeys.E006;
    }
    
    public int addTask(int taskId, int consumption)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        
        if(existsTaskAndRemove(taskId,false))
        {
            return ReturnCodeKeys.E010;
        }
        
        Task task = new Task();
        task.setTaskId(taskId);
        task.setConsumption(consumption);
        hangUpList.add(task);
        return ReturnCodeKeys.E008;
    }
    
    public int deleteTask(int taskId)
    {
        if (taskId <= 0)
        {
            return ReturnCodeKeys.E009;
        }
        
        //如果指定编号的任务未被添加, 返回:任务不存在
        if(!existsTaskAndRemove(taskId,false))
        {
            return ReturnCodeKeys.E012;
        }
        
        // 删除任务
        existsTaskAndRemove(taskId,true);
        return ReturnCodeKeys.E011;
    }
    
    public int scheduleTask(int threshold)
    {
        if(threshold <= 0)
        {
            return ReturnCodeKeys.E002;
        }
        //如果挂起队列中有任务存在
        if(hangUpList.size() > 0)
        {
            bestScheme(map, hangUpList);
            transfer();
            return ReturnCodeKeys.E013;
        }
        else
        {
            int consume = bestScheme(map);
            if(consume <= threshold)
            {
                transfer();
                return ReturnCodeKeys.E013;
            }
            else
            {
                return ReturnCodeKeys.E014;
            }
        }
    }
    
    private int bestScheme(Map<Integer, List<Task>> map2, List<Task> hangUpList2)
    {
        // TODO 最佳迁移方案
        return 0;
    }

    private int bestScheme(Map<Integer, List<Task>> map)
    {
        // TODO 最佳迁移方案
        Map<Integer,Integer> consumeMap = new HashMap<Integer, Integer>();
        for(Iterator<Entry<Integer, List<Task>>> car = map.entrySet().iterator(); car.hasNext();)
        {
            int totalConsume = 0;
            List<Task> list = car.next().getValue();
            for(Task task : list)
            {
                totalConsume += task.getConsumption();
            }
            consumeMap.put(car.next().getKey(), totalConsume);
        }
        return 0;
    }

    private void transfer()
    {
        // TODO 迁移
    }

    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        if(null == tasks)
        {
            return ReturnCodeKeys.E016;
        }
        
        // TODO 查询任务状态
        return ReturnCodeKeys.E015;
    }
    
    /**
     * 相同任务编号任务是否已经被添加
     * @param needRemove true:检查并删除掉这个task | false:只检查
     * @author xuwensheng
     */
    private boolean existsTaskAndRemove(int taskId, boolean needRemove)
    {
        boolean isExist = false;
        //检查挂起队列
        for (Iterator<Task> iter = hangUpList.iterator(); iter.hasNext();)
        {
            if(taskId == iter.next().getTaskId())
            {
                isExist = true;
                if(needRemove)
                {
                    iter.remove();
                }
            }
        }
        
        //检查服务节点
        Collection<List<Task>> coll = map.values();
        for(List<Task> list : coll)
        {
            if(null != list && list.size() > 0)
            {
                for(Iterator<Task> iter = list.iterator(); iter.hasNext();)
                {
                    if(taskId == iter.next().getTaskId())
                    {
                        isExist = true;
                        if(needRemove)
                        {
                            iter.remove();
                        }
                    }  
                }
            }
        }
        
        return isExist;
    }
    
}
