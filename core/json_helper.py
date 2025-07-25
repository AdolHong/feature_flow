import json
import pandas as pd
import numpy as np

class UniversalEncoder(json.JSONEncoder):
    """
    一个可以处理 Pandas DataFrame, Timestamp, NaN, 以及 NumPy 类型的通用编码器。
    """
    def default(self, obj):
        # 处理 Pandas DataFrame
        if isinstance(obj, pd.DataFrame):
            # 将 DataFrame 转换为字典，并添加类型元数据
            return {
                "__type__": "DataFrame",
                "data": obj.to_dict(orient="tight")
            }
        
        # 处理 Pandas Timestamp
        if isinstance(obj, pd.Timestamp):
            return {
                "__type__": "Timestamp",
                "value": obj.isoformat()
            }
        
        # 处理 NaN 和 NaT
        if pd.isna(obj):
            return None
        
        # 处理 NumPy 类型，因为它们不被 json 库识别
        if isinstance(obj, (np.integer, np.floating, np.bool_)):
            return obj.item()

        # 对于其他类型，使用默认编码器
        return super().default(obj)

def universal_decoder(dct):
    """
    根据元数据将字典还原为 Pandas DataFrame 或其他对象。
    """
    if "__type__" in dct:
        data_type = dct["__type__"]
        if data_type == "DataFrame":
            data = dct["data"]
            # split 格式
            if set(data.keys()) == {"index", "columns", "data"}:
                return pd.DataFrame(data["data"], index=data["index"], columns=data["columns"])
            # tight 格式
            elif "index_names" in data or "column_names" in data:
                return pd.DataFrame.from_dict(data, orient="tight")
            else:
                raise ValueError("未知的DataFrame序列化格式")
        if data_type == "Timestamp":
            return pd.Timestamp(dct["value"])
    # 递归处理嵌套字典
    for key, value in dct.items():
        if isinstance(value, dict) and "__type__" in value:
            data_type = value["__type__"]
            if data_type == "DataFrame":
                data = value["data"]
                if set(data.keys()) == {"index", "columns", "data"}:
                    dct[key] = pd.DataFrame(data["data"], index=data["index"], columns=data["columns"])
                elif "index_names" in data or "column_names" in data:
                    dct[key] = pd.DataFrame.from_dict(data, orient="tight")
                else:
                    raise ValueError("未知的DataFrame序列化格式")
            elif data_type == "Timestamp":
                dct[key] = pd.Timestamp(value["value"])
    return dct