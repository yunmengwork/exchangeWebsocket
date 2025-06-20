## [think1.py](./think1.py)
### 思路
这是在假设价差（ask-ask）为正态分布时
考虑一段历史时间的数据，再考虑一段扩展了一段时间窗口的时间的数据
对两组数据进行独立均值t检验，看均值差异
如果均值差异在某一水平下显著，那么认为新添加的数据改变了价差数据的分布
从而推断在新时间窗口下这一段数据是不平稳的

### 参数
其中
observeWindow是观察期窗口尺度，observeWindow太窄会导致识别会很敏感，太宽会导致识别会滞后
timeWindow是扩展数据宽度，timeWindow太窄就不容易识别到分布改变，太宽则会导致识别分布变化敏感度太高
interval是每次迭代的时间间隔
threshold是p-value的阈值

## [think2.py](./think2.py)
### 思路
依旧假设正态分布，考虑当数据分布不变时抽出下一段序列的概率
根据设定一个抽样持续保持在上区间/下区间的阈值比例
例如90%的时间呆在99%的分位点之上，那么认为此时存在上升的趋势，下降趋势判断同理

### 参数
observeWindow是观察期的时间窗口，解释同think1，只是影响并不像think1中那么大。这里一个合适的宽度就可以
timeWindow这里是抽样序列的长度。这个值越小，模型识别越敏感
interval在for循环中每次对序列移动的值
clipLower = 0.01 这里是下截断数值，当概率小于clipLower，取0.01
clipUpper = 0.99 同理于clipLower。\[clipLower,clipUpper\]区间越小，识别越敏感
thresholdRate 需要多少比例的样本呆在上区间或者下区间才能判断分布改变的阈值比例。这个值越小，模型越敏感
lowerThreshold = (clipLower ** (timeWindow * thresholdRate)) * (
    0.5 ** (timeWindow * (1 - thresholdRate))
)
upperThreshold = (clipUpper ** (timeWindow * thresholdRate)) * (
    0.5 ** (timeWindow * (1 - thresholdRate))
)
注：这里的0.5是根据蒙特卡洛模拟得到的（在一个标准正态分布中，不断的随机的取值然后计算其累计概率并相乘，其平均结果是0.5的n（取值次数）次方）