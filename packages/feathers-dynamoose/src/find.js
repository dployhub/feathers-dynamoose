import jsonify from './jsonify';

const shouldUseQuery = (filters, {hashKey, indexKeys}) => {
  const hasGlobalIndex = Object.keys(filters)
    .filter(key => indexKeys.includes(key))
    .filter(indexKey => filters[indexKey].eq || typeof filters[indexKey] === 'string')
    .length > 0;
  const hasHashKey = (filters[hashKey] && filters[hashKey].eq) || typeof filters[hashKey] === 'string';
  return hasHashKey || hasGlobalIndex;
};

const queryParts = (query, keys) => {
  let { $limit, $select, $paginate, ...filters } = query || {};
  $select = $select || [];
  return Object.keys(filters || {}).reduce((acc, key) => {
    const v = filters[key]
    switch (true) {
      case (key === keys.hashKey || (keys.indexKeys && keys.indexKeys.indexOf(key) !== -1)):
        return {...acc, hashQuery: { [key]: { eq: v } }};
      case (key === keys.rangeKey):
        return {...acc, where: {...acc.where, [key]: v}};
      default:
        return {...acc, filters: {...acc.filters, [key]: v}};
    }
  }, { $limit, $select, $paginate, query, hashQuery: {}, where: {}, filters: {} });
}

const performQuery = async (model, params, keys) => {
  const { $limit, $select, $paginate, query, hashQuery, where, filters } = params
  const queryOperation = shouldUseQuery(query, keys) ? model.query(hashQuery) : model.scan(hashQuery)
  
  if (Object.keys(where).length > 0) {
    Object.keys(where).forEach(key => {
      queryOperation.where(key).eq(where[key]);
    });
  }
  if (Object.keys(filters).length > 0) {
    Object.keys(filters).forEach(key => {
      queryOperation.filter(key).eq(filters[key]);
    });
  }
  if (Array.isArray($select) && $select.length > 0) {
    queryOperation.attributes($select);
  }

  /* TODO: Fix pagination
   * Do not implement $limit or pagination because Dynamodb implements it BEFORE filtering, 
   * which means scenarios such as returning 1 single item by hash/range will always fail
   * 
  if ($limit) {
    queryOperation.limit($limit);
  } else if (pagination && pagination.max) {
    queryOperation.limit(pagination.max);
  } else {
    queryOperation.all();
  }
  */

  return queryOperation.exec();
}

const jsonifyResult = schema => (result, paginate) => {
  const { scannedCount, count, timesScanned, timesQueried, lastKey, ...data } = result;
  // data is converted from Array to Object during destructuring
  // we need to convert it back before passing it to jsonify()
  const dataArr = []
  Object.values(data).map(o => dataArr.push(o))
  const jsonData = jsonify(schema)(dataArr)

  // return only data array if paginate is set to false
  return paginate ? { scannedCount, count, timesScanned, timesQueried, lastKey, data: jsonData } : jsonData;
};

const findService = schema => (model, keys) => {
  return {
    find: async params => {
      const parts = queryParts(params.query, keys)
      // check to see if $paginate is set in the query (client)
      // if not, default to params.paginate (server)
      // default to true if paginate is not supplied
      let paginate = typeof parts.$paginate !== 'undefined' 
        ? parts.$paginate
        : (typeof params.paginate !== 'undefined' ? params.paginate : true)
      if (typeof paginate === 'string') {
        paginate = !(paginate.toLowerCase() === 'false' || paginate === '0')
      }
      const result = await performQuery(model, parts, keys)
      return jsonifyResult(schema)(result, paginate);
    }
  };
};

export default findService;
