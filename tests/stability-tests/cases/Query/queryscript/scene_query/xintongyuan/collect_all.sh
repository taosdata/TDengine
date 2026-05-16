repos="TDengine TDinternal taosx taosadapter explorer taos-tools taoskeeperinternal taoskeeper grafanaplugin taos-connector-jdbc taos-connector-python taos-connector-node taos-connector-dotnet taos-connector-rust"


#tdengine_tag="ver-3.1.0.0 ver-3.2.0.0 ver-3.0.0.0 ver-3.3.0.0 ver-1.6.1.7 ver-2.0.0.0 ver-2.4.0.0 ver-2.6.0.0"
#tdinternal_tag="ver-3.1.0.0 ver-3.2.0.1 ver-3.0.0.0 ver-3.3.0.0 ver-1.6.4.1 ver-2.0.0.0 ver-2.4.0.0 ver-2.6.0.0"
#taos_tools_tag="ver-3.1.1.17 ver-3.2.2.0 ver-3.0.5.0 ver-3.3.0.0 2.0.0" 
#taosx_tag="ver-3.1.1.11 ver-3.2.1.0 ver-3.0.5.0 ver-3.3.0.0"
#taoskeeper_tag="ver-3.1.1.12.1114 ver-3.2.1.0 ver-3.0.5.0 ver-3.3.0.0"
#taoskeeperinternal_tag="ver-3.1 ver-3.2.1.0 676fddef7c21d647ea8be842bc5edf8364f2896c ver-3.3.0.0"
#explorer_tag="ver-3.1.1.11 ver-3.2.1.0 ver-3.0.5.0 ver-3.3.0.0"
python_tag="v0.1.0 v2.2.0 v2.5.0 v2.8.0 v2.1.1" #v2.1.1
#grafanaplugin_tag="v3.2.0 v2.0.0 v3.0.0 v3.4.0 v3.7.0 v3.6.0 v3.5.1"
#jdbc_tag="3.2.11 2.0.39 3.0.0 3.3.0 3.5.0 3.6.0"
#rust_tag="taos-v0.2.0 taos-v0.12.3 taos-v0.4.0 taos-v0.8.0 taos-v0.11.0"
#dotnet_tag="v1.0.7 v3.0.0 v3.1.0 9d60e62062ecea726378d457d50f2aeb8722e947"
#node_tag="v3.0.2 ws-v3.1.0"
#taosadapter_tag="ver-3.3.0.0 273b5219f8bcc604e43beebc6f1f95abed85170a f005d7bd83f8509716c07d126f374f1ed2bc59f2 2ca30513fa0de5d682c3ee2afc10d1dcc9a341fb 3d21433f7830c26ed5510f3a93d0affb0472e8d6 4c5b0f93194c8c7f30bcdbea9428f88af6424c7a 71e4c282096695312268653f642468de231da8be"

for repo in $repos
do  
    if [ $repo = "TDengine" ] ; then
        tags=$tdengine_tag
    elif [ $repo = "TDinternal" ] ; then
        tags=$tdinternal_tag
    elif [ $repo = "taos-tools" ]; then
        tags=$taos_tools_tag
    elif [ $repo = "taosx" ]; then
        tags=$taosx_tag
    elif [ $repo = "taosadapter" ]; then
        tags=$taosadapter_tag
    elif [ $repo = "explorer" ]; then
        tags=$explorer_tag
    elif [ $repo = "taoskeeper" ]; then
        tags=$taoskeeper_tag
    elif [ $repo = "taoskeeperinternal" ]; then
        tags=$taoskeeperinternal_tag
    elif [ $repo = "taos-connector-python" ]; then
        tags=$python_tag
    elif [ $repo = "taos-connector-jdbc" ]; then
        tags=$jdbc_tag
    elif [ $repo = "taos-connector-rust" ]; then
        tags=$rust_tag
    elif [ $repo = "taos-connector-dotnet" ]; then
        tags=$dotnet_tag
    elif [ $repo = "taos-connector-node" ]; then
        tags=$node_tag
    elif [ $repo = "grafanaplugin" ]; then
        tags=$grafanaplugin_tag
    fi
    for tag in $tags
    do
        echo "cd $repo :=============================="
        cd $repo && git checkout -f $tag
        echo "git checkout -f $tag"
        echo "Current branch: $(git branch )"
        echo "Current commit:" && git log -1 
        cd ..
        mkdir -p 20250423/log-$repo-$tag
        ./ringer-0.3.15-linux-x86-64  fetch -S $repo/ -O 20250423/log-$repo-$tag  > 20250423/log-$repo-$tag/$repo.log 
    done
done



# core_repo="TDengine TDinternal taosx taosadapter"
# for i in $core_repo
# do
#     tar zcf $i-3.3.0.0-core-20250422.tar.gz log-$i
# done

# core_repo="TDengine TDinternal taosx taosadapter"
# for repo in $core_repo
# do  
#     if [ $repo = "TDengine" ] ; then
#         tags=$tdengine_tag
#     elif [ $repo = "TDinternal" ] ; then
#         tags=$tdinternal_tag
#     elif [ $repo = "taosx" ]; then
#         tags=$taosx_tag
#     elif [ $repo = "taosadapter" ]; then
#         tags=$taosadapter_tag
#     fi
#     for tag in $tags
#     do
#         echo "cd $repo :=============================="
#         cd $repo && git checkout -f $tag
#         echo "git checkout -f $tag"
#         echo "Current branch: $(git branch )"
#         echo "Current commit:" && git log -1 
#         cd ..
#         mkdir -p 20250423/log-$repo-$tag
#         tar zcf 20250423/$repo-$tag-core-20250423.tar.gz 20250423/log-$repo-$tag
#     done
# done


# core_old_repo="2017-01-01 2017-08-18 2019-01-27 2018-06-16 2018-01-18"
# for i in $core_old_repo
# do
#     td_name=$(echo $i | sed 's/-//g')
#     tar zcf TDegine-$td_name-core-20250422.tar.gz log-$i
# done

# tools_repo="explorer taos-tools taoskeeperinternal taoskeeper  grafanaplugin"
# for repo in $tools_repo
# do  
#     if [ $repo = "taos-tools" ]; then
#         tags=$taos_tools_tag
#     elif [ $repo = "explorer" ]; then
#         tags=$explorer_tag
#     elif [ $repo = "taoskeeper" ]; then
#         tags=$taoskeeper_tag
#     elif [ $repo = "taoskeeperinternal" ]; then
#         tags=$taoskeeperinternal_tag
#     elif [ $repo = "grafanaplugin" ]; then
#         tags=$grafanaplugin_tag
#     fi
#     for tag in $tags
#     do
#         echo "cd $repo :=============================="
#         cd $repo && git checkout -f $tag
#         echo "git checkout -f $tag"
#         echo "Current branch: $(git branch )"
#         echo "Current commit:" && git log -1 
#         cd ..
#         mkdir -p 20250423/log-$repo-$tag
#         tar zcf 20250423/$repo-$tag-tools-20250423.tar.gz 20250423/log-$repo-$tag
#     done
# done


# tools_repo="explorer taos-tools taoskeeperinternal taoskeeper  grafanaplugin"
# for i in $tools_repo
# do
#     tar zcf $i-3.3.0.0-tools-20250422.tar.gz log-$i
# done

# connector_repo="taos-connector-jdbc taos-connector-python taos-connector-node taos-connector-dotnet taos-connector-rust"
# for repo in $connector_repo
# do  
#     if [ $repo = "taos-connector-python" ]; then
#         tags=$python_tag
#     elif [ $repo = "taos-connector-jdbc" ]; then
#         tags=$jdbc_tag
#     elif [ $repo = "taos-connector-rust" ]; then
#         tags=$rust_tag
#     elif [ $repo = "taos-connector-dotnet" ]; then
#         tags=$dotnet_tag
#     elif [ $repo = "taos-connector-node" ]; then
#         tags=$node_tag
#     fi
#     for tag in $tags
#     do
#         echo "cd $repo :=============================="
#         cd $repo && git checkout -f $tag
#         echo "git checkout -f $tag"
#         echo "Current branch: $(git branch )"
#         echo "Current commit:" && git log -1 
#         cd ..
#         mkdir -p 20250423/log-$repo-$tag
#         tar_name=$(echo $repo | sed 's/taos-connector-//')
#         tar zcf 20250423/$tar_name-$tag-connector-20250423.tar.gz 20250423/log-$repo-$tag
#     done
# done


# connector_repo="taos-connector-jdbc taos-connector-python taos-connector-node taos-connector-dotnet taos-connector-rust"
# for i in $connector_repo
# do
#     tar_name=$(echo $i | sed 's/taos-connector-//')
#     tar zcf ${tar_name}-3.3.0.0-connector-20250422.tar.gz log-$i
# done

