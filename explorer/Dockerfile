FROM node:22.3.0

# 设置工作目录
WORKDIR /app

# 将本地项目文件复制到容器的工作目录
COPY . .

# 安装项目依赖
RUN npm install -g pnpm && \
    pnpm self-update && \
    pnpm install
 
# 编译 Vue 3 项目
RUN pnpm run build