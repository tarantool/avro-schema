stage('Build'){
    packpack = new org.tarantool.packpack()

    // Ubuntu precise has very old gcc that can't build phf library
    matrix = packpack.filterMatrix(
        packpack.default_matrix,
        {!(it['OS'] == 'ubuntu' && it['DIST'] == 'precise')})

    node {
        checkout scm
        packpack.prepareSources()
    }
    packpack.packpackBuildMatrix('result', matrix)
}
