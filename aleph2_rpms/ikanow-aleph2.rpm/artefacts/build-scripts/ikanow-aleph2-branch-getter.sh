echo "1) Have bamboo.git_repo: ${bamboo.git_repo}"
echo "2) Have bamboo_git_repo: ${bamboo_git_repo}"
echo "3) Have bamboo_planRepository_branch: $bamboo_planRepository_branch"

# CORE
cd Aleph2
echo "CORE: Checking vs $(git branch -a)"
export CORE_BRANCH=$(git branch -a | grep -q $bamboo_git_repo && echo -n $bamboo_git_repo || echo -n master)
echo "Checking out $CORE_BRANCH from $(pwd)"
if [ "$CORE_BRANCH" != "master" ]; then
    git checkout $CORE_BRANCH
fi

# CONTRIB
cd ../Aleph2-contrib
echo "CONTRIB: Checking vs $(git branch -a)"
export CONTRIB_BRANCH=$(git branch -a | grep -q $bamboo_git_repo && echo -n $bamboo_git_repo || echo -n master)
echo "Checking out $CONTRIB_BRANCH from $(pwd)"
if [ "$CONTRIB_BRANCH" != "master" ]; then
    git checkout $CONTRIB_BRANCH
fi