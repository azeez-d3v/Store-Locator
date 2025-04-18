# Import all brand handlers for easy access
from .alive import AliveHandler
from .amcal import AmcalHandler
from .bendigo_ufs import BendigoUfsHandler
from .blooms import BloomsHandler
from .chemist_king import ChemistKingHandler
from .chemist_warehouse import ChemistWarehouseHandler
from .choice import ChoiceHandler
from .community import CommunityHandler
from .dds import DDSHandler
from .footes import FootesHandler
from .friendly_care import FriendlyCareHandler
from .nova import NovaHandler
from .optimal import OptimalHandler
from .pharmasave import PharmasaveHandler
from .ramsay import RamsayHandler
from .revive import ReviveHandler
from .ydc import YdcHandler

__all__ = [
    'amcal',
    'dds',
    'blooms',
    'ramsay',
    'revive',
    'optimal',
    'community',
    'footes',
    'alive',
    'ydc',
    'chemist_warehouse',
    'pharmasave',
    'nova',
    'choice',
    'bendigo_ufs',
    'chemist_king',
    'friendly_care'
]