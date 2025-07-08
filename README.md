# FileLine Expriment Manager

本项目旨在简化科研工作中的数据处理流程（画图流程也被视为数据处理流程的一种）。现有的数据工作流管理平台只能自动化控制流，而不能降低管理数据版本的成本。因此，我们开发了FileLine，一种支持处理操作缓存的科研数据流自动化管理工具。

## 解决的核心问题

- 原始数据格式各样，难以统一处理流程。

    实验过程中产生的原始数据格式各种各样，为了满足实验需求更是会引入大量自定义的逻辑，都需要逐一编写处理代码。因此通过简化新文件处理功能的开发难度，提升数据处理效率，用最少的开发量实现数据处理流程的自动化。

- 自动化管理数据处理流程。

    实验过程中，会产生大量数据处理操作，这些操作的输入，参数，以及处理代码的版本都各不相同，手动管理这些操作会耗费大量时间。因此通过自动化管理数据处理流程，降低管理成本。

- 自动缓存已处理过的操作，避免重复处理。

    实验过程中产生的很多操作，都有相同的输入，参数，以及处理代码的版本，因此可以通过自动缓存已处理过的操作，避免重复处理，提升数据处理效率。

- 数据文件版本管理困难。

    实验过程中，会产生大量同名的原始数据，中间处理数据，如果都靠手动管理版本，会造成管理困难。因此通过自动代码历史记录保存，可追溯数据处理过程，提升数据处理效率。

## 核心功能

- 自定义yaml配置文件，实现数据处理流程的自动化。
- 自动代码历史记录保存，可追溯数据处理过程。
- 自动缓存已处理过的操作，若数据处理操作的输入、参数、以及代码版本不变，则可避免重复处理。
- 简化扩展新文件处理功能的开发难度，只需要带上注册注解即可。
- 将所有文件处理操作分为单输入单输出，多输入单输出两类，通过自定义pipeline组合，实现任意数据处理流程的自动化。

## 竞品分析

| 维度                 |         具体分类         | FileLine(本项目)                                             | snakemake数据分析工作流管理                                  | Apache Airflow 工作流管理工具                                | KNIME数据分析平台                                            |
| -------------------- | :----------------------: | :----------------------------------------------------------- | :----------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------ |
|                      |       **基本介绍**       | 见本项目其他部分                                             | 用于构建和管理数据分析工作流程的工具                         | 以编程方式编写，安排和监视工作流的平台                       | 拖拽式开源数据管理分析平台                                   |
| **自动化能力**       |  **数据处理流程自动化**  | ✅支持 YAML 配置自动化数据处理流程，支持通过全局配置文件替换关键字 | ✅支持自定义rule文件，规定一系列的rule（包括输入，输出和待执行命令）实现数据处理流水线化 | ✅使用 **DAG** 描述任务间的逻辑关系，每个任务（Task）作为图的节点，依赖关系通过有向边连接 | ✅使用拖拽式的节点描述任务间的逻辑管理，支持任意输入输出，支持循环 |
| **版本控制能力**     |     **版本追溯能力**     | ✅全处理链路中间文件历史记录保存，支持通过输入参数和代码版本精确回溯数据处理路径 | ❌                                                            | ❌                                                            | ❌                                                            |
|                      |    **缓存与复用机制**    | ✅自动检测输入/参数/代码版本一致性，避免重复计算（如相同原始数据的多次绘图） | ❌                                                            | ❌                                                            | ❌                                                            |
| **扩展能力**         |    **扩展开发友好性**    | ✅通过注解注册新处理函数，支持 Python 快速开发（如自定义数据清洗算法） | ✅支持通过shell命令调用shell指令，通过script命令，调用任意python脚本 | ✅每个节点支持执行预先封装的shell、python、mysql、http等不同类型的命令 | ✅支持集成py thon环境后，向工作流图中插入py节点执行任意指令   |
|                      | **任意Python代码兼容性** | ✅支持单输入单输出/多输入单输出类型（文件）的处理函数         | ✅可以实现类似make的调用和执行，可以执行任意代码              | ❌继承现有的各类operator基类后，可以execute任意python代码，若无法修改则不行 | ✅支持多输入/多输出(通过传给py脚本输入表和输出表)             |
| **兼容性**           |       **使用场景**       | ✅无需图形界面                                                | ✅无需图形界面                                                | ✅无需图形界面（通过网页可视化）                              | ❌需要图形界面                                                |
| **并行与可视化能力** |     **自动并行处理**     | ❌                                                            | ✅                                                            | ✅                                                            | ✅（需要设置并行度）                                          |
|                      |        图形化界面        | ❌                                                            | ❌                                                            | ✅（网页）                                                    | ✅                                                            |
|                      |      预定义绘图算子      | ✅                                                            | ❌                                                            | ❌                                                            | ✅                                                            |



## 使用方法

- 确认所需处理过程是否实现(processes目录下)，且使用了ProcessorRegistry.register注解。

- 将各个处理步骤组合成pipeline，并在yaml配置文件中配置。

- 运行main.py，即可实现数据处理流程的自动化。(详细命令可通过python main.py --help查看)

## 参考样例

- 首先创建并使用新实验

    ```
        python main.py experiment create demo
        python main.py experiment use demo
    ```

- 流水线配置文件见examples目录下, 包括单输入单输出样例（test.yaml）, 多输入单输出绘图样例（loss_curve.yaml）,占位符控制样例（loss_curve_var.yaml）及其对应全局配置文件（loss_curve_var.global）。所有被调用的processor应当提前注册到ProcessorRegistry中（processes目录下）。

- 运行流水线

    ```
        Usage: main.py pipeline run [OPTIONS] CONFIG_FILE

        运行带文件加载的流水线

        Options:
        --global-config PATH  全局配置文件路径（包含变量定义）
        --debug / --no-debug
        --help                Show this message and exit.
    ```

## 功能介绍

- data 数据相关功能, 负责追踪数据的历史版本，管理数据标签等功能

    ```
    add           添加原始数据文件
        main.py data add [OPTIONS] FILE_PATH
            Options:
                --description TEXT  数据描述信息

    list-recent   显示最近的实验数据条目
        main.py data list-recent [OPTIONS]
            Options:
                --limit INTEGER  显示最近的记录数量  [default: 5]

    show          根据条件展示数据条目
        main.py data show [OPTIONS]
            Options:
                -i, --id INTEGER             筛选指定ID的数据
                -t, --tag TEXT               筛选包含指定标签的数据
                --type [raw|processed|plot]  按数据类型筛选
                --limit INTEGER              最大显示条目数  [default: 20]
    ```

- experiment 实验相关功能，支持为每个实验建立独立的数据库和目录，方便调试运行/有效绘图分离，以及不同实验之间的隔离。

    ```
    create  创建新实验
        main.py experiment create [OPTIONS] NAME
            Options:
                --description TEXT  实验描述
    delete  删除实验
        main.py experiment delete [OPTIONS] NAME
    list    列出所有实验
        main.py experiment list [OPTIONS]
    use     切换当前实验
        main.py experiment use [OPTIONS] NAME
    ```

- pipeline 流水线相关功能， 支持将一系列数据处理操作组合成流水线，并自动缓存已处理过的操作，避免重复处理。所有的prcessor都应当使用ProcessorRegistry.register注解进行注册, 并实现在proceses目录下。

    ```
    run
        main.py pipeline run [OPTIONS] CONFIG_FILE
            Options:
                --global-config PATH  全局配置文件路径（包含变量定义）
                --debug / --no-debug
    ```
- process 单独调用处理操作。用于测试单processor的接口。

    ```
    run
        main.py process run [OPTIONS] PROCESSOR_NAME INPUT_IDS
            Options:
                -p, --param TEXT  处理参数，格式：key=value
    ···

## 开发教程

- 实现自定义processor

    - 遵循约定的参数格式：输入文件描述，输出文件路径，以及其他任意的处理参数。

    - 使用ProcessorRegistry.register注解进行注册。指明processord的名称，processor的类型（单输入单输出single/多输入单输出multi），输出文件的后缀名类型（默认.csv）。

- 在pipeline配置文件中调用自定义processor

    - 首先配置initial_load部分的原始输入文件列表，通过include部分，指定一个或多组使用通配符*和?、[]等匹配的Posix路径名模式，匹配到的文件将被加载到pipeline的初始输入文件列表中，保存为initial变量。支持通过regex子表达式进行进一步匹配，实现完备的路径匹配。

    - 随后依次定义pipeline的各个processor，每个stage包含一个processor的名称，以及该processor的输入文件变量名，输出变量名，以及其他任意的处理参数。（要求需要符合single和multi两种类型processor的输入输出要求）

    - 最后定义导出文件名，将最终输出的文件导出为想要的名称（因为系统内部使用uuid保存以避免重名情况，所以需要用到导出功能实现导出到想要的文件名）


## TODO

- 通过支持自定义输入匹配函数，支持更复杂的输入路径模式匹配，比如匹配一个目录下最新的时间戳。

- 支持自动根据最终导出的文件的处理历史生成图示, 同时支持导出其对应的原始数据，便于追溯数据处理过程。 (已经支持导出原始数据， 集成在data trace命令中，暂时未更新文档)

- 完善并增加更多常用的绘图函数，数据处理函数等，兼容通用化。