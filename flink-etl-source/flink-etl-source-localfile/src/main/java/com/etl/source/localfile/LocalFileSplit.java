package com.etl.source.localfile;

import com.etl.core.source.BaseSourceSplit;
import lombok.Getter;

import java.io.File;
import java.io.Serializable;
import java.util.List;

/**
 * 文件分片
 * 一个文件对应一个分片
 */
@Getter
public class LocalFileSplit implements BaseSourceSplit, Serializable {

    private static final long serialVersionUID = 1L;

    /** 分片 ID = 文件路径 */
    private final String splitId;

    /** 文件绝对路径 */
    private final String filePath;

    /** 文件名 */
    private final String fileName;

    /** 字段名列表 */
    private final List<String> fields;

    /**
     * 构造函数
     *
     * @param filePath 文件绝对路径
     * @param fields 字段名列表
     */
    public LocalFileSplit(String filePath, List<String> fields) {
        this.filePath = filePath;
        this.fileName = new File(filePath).getName();
        this.splitId = filePath;
        this.fields = fields;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    @Override
    public String toString() {
        return "LocalFileSplit{" +
                "splitId='" + splitId + '\'' +
                ", fileName='" + fileName + '\'' +
                ", fields=" + fields +
                '}';
    }
}