package com.etl.core.source;

import com.etl.core.source.base.BaseSplitState;

/**
 * 范围分片状态
 * 用于跟踪范围分片的读取进度
 *
 * <p>当前实现为简单包装，未来可扩展支持：
 * <ul>
 *   <li>记录已读取的位置</li>
 *   <li>支持断点续读</li>
 *   <li>记录读取统计信息</li>
 * </ul>
 */
public class RangeSplitState extends BaseSplitState<RangeSplit> {

    private static final long serialVersionUID = 1L;

    /**
     * 当前读取位置
     * 用于断点续读
     */
    private long currentPosition;

    /**
     * 已读取的记录数
     */
    private long recordsRead;

    /**
     * 构造函数
     *
     * @param split 范围分片
     */
    public RangeSplitState(RangeSplit split) {
        super(split);
        this.currentPosition = split.getStart();
        this.recordsRead = 0;
    }

    /**
     * 获取当前读取位置
     *
     * @return 当前位置
     */
    public long getCurrentPosition() {
        return currentPosition;
    }

    /**
     * 更新当前读取位置
     *
     * @param position 新位置
     */
    public void setCurrentPosition(long position) {
        this.currentPosition = position;
    }

    /**
     * 获取已读取记录数
     *
     * @return 已读取记录数
     */
    public long getRecordsRead() {
        return recordsRead;
    }

    /**
     * 增加已读取记录数
     *
     * @param count 增加的数量
     */
    public void addRecordsRead(long count) {
        this.recordsRead += count;
    }

    /**
     * 判断分片是否读取完成
     *
     * @return 是否完成
     */
    public boolean isFinished() {
        return currentPosition > getSplit().getEnd();
    }

    @Override
    public String toString() {
        return "RangeSplitState{" +
                "split=" + getSplit() +
                ", currentPosition=" + currentPosition +
                ", recordsRead=" + recordsRead +
                '}';
    }
}