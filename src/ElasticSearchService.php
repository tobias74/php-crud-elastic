<?php
namespace Zeitfaden\Elasticsearch;

class ElasticSearchService
{

  protected $mapper;
  protected $config;

  public function __construct($config, $mapper)
  {
      $this->config = $config;
      $this->mapper = $mapper;
  }

  protected function getConfig()
  {
    return $this->config;  
  }

  protected function getMapper()
  {
    return $this->mapper;
  }

  protected function getClient()
  {
    $hosts = [
      $this->getConfig()->getElasticSearchHost()
    ];
    
    $clientBuilder = \Elasticsearch\ClientBuilder::create();   
    $clientBuilder->setHosts($hosts);                          
    $client = $clientBuilder->build();       
    
	  return $client;
  }

  public function createIndex()
  {
    $indexParams = $this->getMapper()->getCreateIndexCommand();
    $this->getClient()->indices()->create($indexParams);
  }

  protected function getIndexName()
  {
    return $this->getMapper()->getIndexName();
  }

  protected function getTypeName()
  {
    return $this->getMapper()->getTypeName();
  }

  protected function mapEntityToHash($station)
  {
    return $this->getMapper()->mapEntityToHash($station);
  }

  public function indexEntity($entity)
  {
    $params = array();
    $params['body']  = $this->mapEntityToHash($entity);;
    $params['index'] = $this->getIndexName();
    $params['type']  = $this->getTypeName();
    $params['id']    = $entity->getId();

    $ret = $this->getClient()->index($params);
    
    $this->getClient()->indices()->refresh(array(
      'index' => $this->getIndexName()
    ));
  }


  public function deleteEntity($entity)
  {
    $deleteParams = array();
    $deleteParams['index'] = $this->getIndexName();
    $deleteParams['type'] = $this->getTypeName();
    $deleteParams['id'] = $entity->getId();
    $deleteParams['refresh'] = true;
    $retDelete = $this->getClient()->delete($deleteParams);
  }





  public function searchBySpecification($elasticSpec, $aggregations = false)
  {
    
    $includeHashtags = false;
    
    $filterCriteria = $elasticSpec['criteria'];
    $criteriaVisitor = new ElasticsearchFilterCriteriaVisitor( $this->getMapper() );
    $filterCriteria->acceptVisitor($criteriaVisitor);
    $filter = $criteriaVisitor->getArrayForCriteria($filterCriteria);
    
    $sorting = $elasticSpec['sorting'];
    
    if ($sorting['sortType'] === 'byDistanceToPin')
    {
      $sort = array(
        '_geo_distance' => array(
            $sorting['sortField'] => array(
            'lat' => floatval($sorting['latitude']), 
            'lon' => floatval($sorting['longitude'])
        ),
          'order' => 'asc',
          'unit' => 'km'
        ),
        'id' => array(
        'order' => 'asc'
        )
      );
    }
    else if ($sorting['sortType'] === 'byField')
    {
      $sort = array(
        $sorting['sortField'] => $sorting['sortOrder'],
        'id'=> 'asc'  
      );    
    }
    else {
      $sort = array(
        'id'=> 'asc'  
      );    
    }
    
    $from = $elasticSpec['offset'];
    $size = $elasticSpec['limit'];

    $query = array(
      'query' => $filter,
      'sort' => $sort,
      'from' => $from,
      'size' => $size
    );


    if (isset($elasticSpec['search_after']))
    {
      $query['search_after'] = $elasticSpec['search_after'];
    }

    if ($aggregations !== false)
    {
      $query['aggs'] = $aggregations;
    }

	  $params = array();
	  $params['index'] = $this->getIndexName();
	  $params['type'] = $this->getTypeName();
	  $params['body'] = $query;

    //echo (json_encode($query));

	  $responseArray = $this->getClient()->search($params);

    $finalResponse = array();

    foreach ($responseArray['hits']['hits'] as $index => $data)
    {
      $entity = $this->mapHashToEntity($data);
      $finalResponse[$index] = $entity;
    }

    $returnHash = array(
      'entities' => $finalResponse
    );


    if ($aggregations !== false)
    {
      $returnHash['aggregations'] = $responseArray['aggregations'];
    }

    return $returnHash;
  }


  protected function mapHashToEntity($stationData)
  {
    return $this->getMapper()->mapHashToEntity($stationData);
  }







}
