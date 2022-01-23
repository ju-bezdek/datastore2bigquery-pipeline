import datetime
import logging
import os
from typing import Dict, List, Union,Optional,Text,Iterable

from apache_beam import PTransform, Create, DoFn

from apache_beam.io.gcp.datastore.v1new.types import Query, Entity


from google.cloud.datastore.helpers import GeoPoint


from apache_beam.io.gcp.datastore.v1new.types import Entity, Key


from google.api_core.gapic_v1 import client_info
from google.cloud import environment_vars
from google.cloud.datastore import __version__
from google.cloud.datastore import client


from apache_beam.version import __version__ as beam_version
from cachetools.func import ttl_cache



class EntKey:
    """Represents a enity kind and id """

    def __init__(self, kind, id_or_name):
        self.kind = kind
        self.id_or_name = id_or_name


def decode_key(key: Key) -> EntKey:
    """Extracts key from the Key structure provided by beam (saves flat_path field which is a tupple of original key data)

    Args:
        entity (Entity): Beam version of GCP entity

    Returns:
        EntKey: Structured key data
    """

    if len(key.path_elements) > 2:
        kind = key.path_elements[2]
        id_or_name = key.path_elements[3]
    else:
        kind = key.path_elements[0]
        id_or_name = key.path_elements[1]

    return EntKey(kind, id_or_name)


def entity_to_json(entity:Entity):
    """
    Convert datastore entity to JSON

    :param entity: datastore entity
    :return: dictionary with the entity
    """
    entKey = decode_key(entity.key)
    entity_dict = {
        '_key': entKey.id_or_name,
        '_kind': entKey.kind
    }

    for k, v in entity.properties.items():
        if isinstance(v, datetime.datetime):
            entity_dict[k] = str(v)
        elif isinstance(v, GeoPoint):
            entity_dict[k] = {'lat': str(v.latitude), 'lng': str(v.longitude)}
        else:
            entity_dict[k] = v

    return entity_dict

    



class EntityFilters:
    """EntityFilter keeps information about the filtering of given kind."""

    def __init__(self, field_name: str, since_time: str):
        """Create instance of entity filter

        Args:
            field_name (str): name of timestamp_field
            since_time (str): time since we should take data
        """
        self.field_name = field_name
        self.since_time = since_time

    def get_filter(self) -> tuple:
        """Get filter configuration"""

        return [
            (self.field_name, '>=', self.since_time)
        ]


def get_entity_filters(ents_timestamp_fields: Dict[str, str], since_time:datetime) -> Dict[str, EntityFilters]:
    """Creates sturctured filter definitions from dictionary of 
       filtered fields for each entity and 
       time from which to extract data

    Args:
        ents_timestamp_fields (Dict[str, str]): Mapping of ent kind and timestamp field that wil determine the ETL slice
        since_time (datetime): since when to extract data

    Returns:
        Dict[str, EntityFilter]: Filters for each entity
    """
    filter_entities = {}
    
    for kind_name, field_name in ents_timestamp_fields.items():
        filter_entities[kind_name] = EntityFilters( field_name["field"], since_time)
    
    
    return filter_entities





class CreateEntitiesLoadQuery(DoFn):
    """
    Create a query for getting all entities the kind taken.
    """

    def __init__(self,project_id: str, entity_filtering: Dict[str, EntityFilters]):        
        self.project_id = project_id
        self.entity_filtering = entity_filtering
        

    def process(self, kind_name, **kwargs):
        """
        :param **kwargs:
        :param kind_name: a kind name
        :return: [Query]
        """

        logging.info(f'CreateQuery.process {kind_name} {kwargs}')

        q = Query(kind=kind_name, project=self.project_id)
        if kind_name in self.entity_filtering:
            q.filters = self.entity_filtering[kind_name].get_filter()

        logging.info(f'Query for kind {kind_name}: {q}')

        return [q]



class QueryFn(DoFn):
    """Fixed version of original apache_beam.io.gcp.datastore.v1new.datastoreio.ReadFromDatastore._QueryFn function
    due to reporetd BUG: https://issues.apache.org/jira/browse/BEAM-9245
    
    Args:
        DoFn ([type]): [description]
    """
    def process(self, query, *unused_args, **unused_kwargs):
      if query.namespace is None:
        query.namespace = ''
        _client = get_client(query.project, query.namespace)
        client_query = query._to_client_query(_client)

        for client_entity in client_query.fetch(query.limit):
          yield Entity.from_client_entity(client_entity)


class Key(object):
  def __init__(self,
               path_elements,  # type: List[Union[Text, int]]
               parent=None,  # type: Optional[Key]
               project=None,  # type: Optional[Text]
               namespace=None  # type: Optional[Text]
               ):
    """
    Represents a Datastore key.

    The partition ID is represented by its components: namespace and project.
    If key has a parent, project and namespace should either be unset or match
    the parent's.

    Args:
      path_elements: (list of str and int) Key path: an alternating sequence of
        kind and identifier. The kind must be of type ``str`` and identifier may
        be a ``str`` or an ``int``.
        If the last identifier is omitted this is an incomplete key, which is
        unsupported in ``WriteToDatastore`` and ``DeleteFromDatastore``.
        See :class:`google.cloud.datastore.key.Key` for more details.
      parent: (:class:`~apache_beam.io.gcp.datastore.v1new.types.Key`)
        (optional) Parent for this key.
      project: (str) Project ID. Required unless set by parent.
      namespace: (str) (optional) Namespace ID
    """
    # Verification or arguments is delegated to to_client_key().
    self.path_elements = tuple(path_elements)
    self.parent = parent
    self.namespace = namespace
    self.project = project

  @staticmethod
  def from_client_key(client_key):
    return Key(
        client_key.flat_path,
        project=client_key.project,
        namespace=client_key.namespace)

  def to_client_key(self):
    """
    Returns a :class:`google.cloud.datastore.key.Key` instance that represents
    this key.
    """
    parent = self.parent
    if parent is not None:
      parent = parent.to_client_key()
    return Key(
        *self.path_elements,
        parent=parent,
        namespace=self.namespace,
        project=self.project)

  def __eq__(self, other):
    if not isinstance(other, Key):
      return False
    if self.path_elements != other.path_elements:
      return False
    if self.project != other.project:
      return False
    if self.parent is not None and other.parent is not None:
      return self.parent == other.parent

    return self.parent is None and other.parent is None

  __hash__ = None  # type: ignore[assignment]

  def __repr__(self):
    return '<%s(%s, parent=%s, project=%s, namespace=%s)>' % (
        self.__class__.__name__,
        str(self.path_elements),
        str(self.parent),
        self.project,
        self.namespace)


class Entity(object):
  def __init__(
      self,
      key,  # type: Key
      exclude_from_indexes=()  # type: Iterable[str]
  ):
    """
    Represents a Datastore entity.

    Does not support the property value "meaning" field.

    Args:
      key: (Key) A complete Key representing this Entity.
      exclude_from_indexes: (iterable of str) List of property keys whose values
        should not be indexed for this entity.
    """
    self.key = key
    self.exclude_from_indexes = set(exclude_from_indexes)
    self.properties = {}

  def set_properties(self, property_dict):
    """Sets a dictionary of properties on this entity.

    Args:
      property_dict: A map from property name to value. See
        :class:`google.cloud.datastore.entity.Entity` documentation for allowed
        values.
    """
    self.properties.update(property_dict)

  @staticmethod
  def from_client_entity(client_entity):
    res = Entity(
        Key.from_client_key(client_entity.key),
        exclude_from_indexes=set(client_entity.exclude_from_indexes))
    for name, value in client_entity.items():
      if isinstance(value, Key):
        value = Key.from_client_key(value)
      if isinstance(value, Entity):
        if value.key:
          value = Entity.from_client_entity(value)
        else:
          value = {k:v for k,v in value.items()}
      res.properties[name] = value
    return res

  def to_client_entity(self):
    """
    Returns a :class:`google.cloud.datastore.entity.Entity` instance that
    represents this entity.
    """
    
    res = Entity(
        key=self.key.to_client_key(),
        exclude_from_indexes=tuple(self.exclude_from_indexes))
    for name, value in self.properties.items():
      if isinstance(value, Key):
        if not value.project:
          value.project = self.key.project
        value = value.to_client_key()
      if isinstance(value, Entity):
        if not value.key.project:
          value.key.project = self.key.project
        value = value.to_client_entity()
      res[name] = value
    return res

  def __eq__(self, other):
    if not isinstance(other, Entity):
      return False
    return (
        self.key == other.key and
        self.exclude_from_indexes == other.exclude_from_indexes and
        self.properties == other.properties)

  __hash__ = None  # type: ignore[assignment]

  def __repr__(self):
    return "<%s(key=%s, exclude_from_indexes=%s) properties=%s>" % (
        self.__class__.__name__,
        str(self.key),
        str(self.exclude_from_indexes),
        str(self.properties))


@ttl_cache(maxsize=128, ttl=3600)
def get_client(project, namespace):
  """Returns a Cloud Datastore client."""
  _client_info = client_info.ClientInfo(
      client_library_version=__version__,
      gapic_version=__version__,
      user_agent=f'beam-python-sdk/{beam_version}')
  _client = client.Client(
      project=project, namespace=namespace, client_info=_client_info)
  # Avoid overwriting user setting. BEAM-7608
  if not os.environ.get(environment_vars.GCD_HOST, None):
    _client.base_url = 'https://batch-datastore.googleapis.com'  # BEAM-1387
  return _client