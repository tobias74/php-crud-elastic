<?php
namespace PhpCrudElastic;

class ElasticSearchService
{

  protected $mapper;
  protected $config;

  public function __construct($config, $mapper)
  {
      $this->config = $config;
      $this->mapper = $mapper;
  }

    protected function getEntityWords($words, $offset)
    {
        $commandWords = array_slice($words,$offset);
        $entityWords = array();
        $lastWord="";
        foreach ($commandWords as $word){
            if (($word === "And") || ($word === "Or")) {
                $entityWords[] = $lastWord;
                $entityWords[] = $word;
                $lastWord = "";
            }
            else {
                $lastWord.=$word;
            }
        }
        $entityWords[]=$lastWord;
        return $entityWords;   
    }

    protected function makeSpecification($entityWords, $arguments)
    {
        if ( (array_search('And',$entityWords)!==false) && (array_search('Or',$entityWords)!==false) )
        {
            throw new \ErrorException("Error: Combined Or and And? ".print_r($entityWords, true));
        }
        else if (array_search('And',$entityWords)!==false)
        {
            $operation = 'And';
        }
        else if (array_search('Or',$entityWords)!==false)
        {
            $operation = 'Or';
        }
        else
        {
            // this is ok, it's just one word
        }
        
        $combinedWords = array_merge(array_diff($entityWords, ['And','Or']));

        $criteriaMaker = new \PhpVisitableSpecification\CriteriaMaker();
        foreach ($combinedWords as $index => $entityWord)
        {
            if (!isset($criteria))
            {
                $criteria = $criteriaMaker->equals(lcfirst($combinedWords[$index]), $arguments[$index]);
            }
            else
            {
                $command = 'logical'.$operation;
                $criteria = $criteria->$command( $criteriaMaker->equals(lcfirst($combinedWords[$index]), $arguments[$index]) );
            }
        }
        
        return $criteria;
        
    }

    public function __call($name, $arguments)
    {
        $words = $this->splitByCamelCase($name);

        if (($words[0] === "get") && ($words[1] === "By")){
            $entityWords = $this->getEntityWords($words,2);
            $criteria = $this->makeSpecification($entityWords, $arguments);
            return $this->getBySpecification($criteria);
        }
        else if (($words[0] === "get") && ($words[1] === "One") && ($words[2] === "By")){
            $entityWords = $this->getEntityWords($words,3);
            $criteria = $this->makeSpecification($entityWords, $arguments);
            return $this->getOneBySpecification($criteria);
        }
        else 
        {
            throw new \ErrorException('Method not found '.$name);
        }
    }


    protected function splitByCamelCase($camelCaseString) 
    {
        $re = '/(?<=[a-z]|[0-9])(?=[A-Z])/x';
        $a = preg_split($re, $camelCaseString);
        return $a;
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
      $this->getConfig()['elasticSearchHost']
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

  public function getIndexName()
  {
    return $this->getMapper()->getIndexName();
  }

  public function getTypeName()
  {
    return $this->getMapper()->getTypeName();
  }

  protected function mapEntityToHash($station)
  {
    return $this->getMapper()->mapEntityToHash($station);
  }


  public function indexEntityWithoutRefresh($entity)
  {
    $params = array();
    $params['body']  = $this->mapEntityToHash($entity);;
    $params['index'] = $this->getIndexName();
    $params['type']  = $this->getTypeName();
    $params['id']    = $entity->getId();
    //$params['client']['future'] = 'lazy';

    $returnValue = $this->getClient()->index($params);

    return $returnValue;
  }

  public function indexEntity($entity)
  {
    $this->indexEntityWithoutRefresh($entity);
    
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




  public function getOneBySpecification($elasticSpec)
  {
    $entities = $this->getBySpecification($elasticSpec);
    if (count($entities) > 1)
    {
      throw \Exception('too many results');
    }
    else if (count($entities) === 0)
    {
      throw new \Exception('no results');
    }
    else 
    {
      return $entities[0];
    }
  }


  protected function getFilter($filterCriteria)
  {
    $criteriaVisitor = new ElasticsearchFilterCriteriaVisitor( $this->getMapper() );
    $filterCriteria->acceptVisitor($criteriaVisitor);
    $filter = $criteriaVisitor->getArrayForCriteria($filterCriteria);
    return $filter;    
  }

  protected function getDeleteParams($elasticSpec)
  {
    $query = array(
      'query' => $this->getFilter($elasticSpec['criteria'])
    );

	  $params = array();
	  $params['index'] = $this->getIndexName();
	  $params['type'] = $this->getTypeName();
	  $params['body'] = $query;
    
    return $params;
  }

  protected function getSearchParams($elasticSpec)
  {
    if (isset($elasticSpec['sorting']))
    {
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
    }
    else
    {
      $sort = array();
    }
    
    $from = $elasticSpec['offset'];
    $size = $elasticSpec['limit'];

    $query = array(
      'query' => $this->getFilter($elasticSpec['criteria']),
      'sort' => $sort,
      'from' => $from,
      'size' => $size
    );


    if (isset($elasticSpec['search_after']))
    {
      $query['search_after'] = $elasticSpec['search_after'];
    }

	  $params = array();
	  $params['index'] = $this->getIndexName();
	  $params['type'] = $this->getTypeName();
	  $params['body'] = $query;
    
    
    return $params;
  }

  public function getBySpecification($elasticSpec)
  {
    $params = $this->getSearchParams($elasticSpec);

    $responseArray = $this->getClient()->search($params);

    $finalResponse = array();

    foreach ($responseArray['hits']['hits'] as $index => $data)
    {
      $entity = $this->mapHashToEntity($data);
      $finalResponse[$index] = $entity;
    }

    return $finalResponse;
  }


  protected function mapHashToEntity($stationData)
  {
    return $this->getMapper()->mapHashToEntity($stationData);
  }



  public function deleteBySpecification($elasticSpec, $params = array())
  {
    $params = array_merge($this->getDeleteParams($elasticSpec), $params);
	  $result = $this->getClient()->deleteByQuery($params);
	  return $result;
  }





  public function aggregate($criteria, $aggregation)
  {
    $criteriaVisitor = new ElasticsearchFilterCriteriaVisitor( $this->getMapper() );
    $criteria->acceptVisitor($criteriaVisitor);
    $filter = $criteriaVisitor->getArrayForCriteria($criteria);

    
    $aggregationHash = [
      "field" => $this->getMapper()->getColumnForField($aggregation['field'])
    ];
    
    if (isset($aggregation['size']))
    {
      $aggregationHash['size'] = $aggregation['size'];
    }


    $query = array();
    $query['aggs'] = [
      $this->getTypeName() => [
        "filter" => $filter,
        "aggs" => [
          "myAggName" => [
            $aggregation['type'] => $aggregationHash
          ]
        ]
      ]
    ];
    $params = array();
    $params['index'] = $this->getIndexName();
    $params['type'] = $this->getTypeName();
    $params['body'] = $query;
    $responseArray = $this->getClient()->search($params);
    return $responseArray['aggregations'][$this->getTypeName()]['myAggName'];
  }


  public function aggregatePassThroughDirectly($criteria, $aggregation)
  {
    $criteriaVisitor = new ElasticsearchFilterCriteriaVisitor( $this->getMapper() );
    $criteria->acceptVisitor($criteriaVisitor);
    $filter = $criteriaVisitor->getArrayForCriteria($criteria);

    
    $query = array();
    $query['query'] = $filter;
    $query['aggs'] = $aggregation;
    
    $params = array();
    $params['index'] = $this->getIndexName();
    $params['type'] = $this->getTypeName();
    $params['body'] = $query;
    
    error_log(json_encode($params));
    
    $responseArray = $this->getClient()->search($params);
    return $responseArray['aggregations'];
  }

  public function aggregatePassThrough($criteria, $aggregation)
  {
    $criteriaVisitor = new ElasticsearchFilterCriteriaVisitor( $this->getMapper() );
    $criteria->acceptVisitor($criteriaVisitor);
    $filter = $criteriaVisitor->getArrayForCriteria($criteria);

    
    $query = array();
    $query['aggs'] = [
      $this->getTypeName() => [
        "filter" => $filter,
        "aggs" => $aggregation
      ]
    ];
    $params = array();
    $params['index'] = $this->getIndexName();
    $params['type'] = $this->getTypeName();
    $params['body'] = $query;
    $responseArray = $this->getClient()->search($params);
    return $responseArray['aggregations'][$this->getTypeName()];
  }

  public function getColumnForField($field)
  {
    return $this->getMapper()->getColumnForField($field);
  }





}
