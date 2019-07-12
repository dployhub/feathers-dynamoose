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
  let { $limit, $select, $startAt, ...filters } = query || {};
  $select = $select || [];
  return Object.keys(filters || {}).reduce((acc, key) => {
    const v = filters[key]
    switch (true) {
      case (key === keys.hashKey):
        return {...acc, hashQuery: { [key]: { eq: v } }};
      case (key === keys.rangeKey):
        return {...acc, where: {...acc.where, [key]: v}};
      default:
        return {...acc, filters: {...acc.filters, [key]: v}};
    }
  }, { $limit, $select, hashQuery: {}, where: {}, filters: {} });
}

const performQuery = async (model, params, keys) => {
  const { $limit, $select, hashQuery, where, filters } = queryParts(params.query, keys)
  const queryOperation = shouldUseQuery(filters, keys) ? model.query(hashQuery) : model.scan(hashQuery)
  
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
  const { scannedCount, count, timesScanned, lastKey, ...data } = result;
  // data is converted from Array to Object during destructuring
  // we need to convert it back before passing it to jsonify()
  const dataArr = []
  Object.values(data).map(o => dataArr.push(o))
  const jsonData = jsonify(schema)(dataArr)
  return paginate ? { scannedCount, count, timesScanned, lastKey, data: jsonData } : jsonData;
};

const findService = schema => (model, keys) => {
  return {
    find: async params => {
      const result = await performQuery(model, params, keys)
      return jsonifyResult(schema)(result, params.paginate !== false);
    }
  };
};

export default findService;
