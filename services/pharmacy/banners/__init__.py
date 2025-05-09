# Import all AU brand handlers
from .alive import AliveHandler
from .amcal import AmcalHandler
from .bendigo_ufs import BendigoUfsHandler
from .blooms import BloomsHandler
from .chemist_hub import ChemistHubHandler
from .chemist_king import ChemistKingHandler
from .chemist_warehouse import ChemistWarehouseHandler
from .choice import ChoiceHandler
from .community import CommunityHandler
from .dds import DDSHandler
from .direct_chemist import DirectChemistHandler
from .footes import FootesHandler
from .friendly_care import FriendlyCareHandler
from .nova import NovaHandler
from .optimal import OptimalHandler
from .pharmasave import PharmasaveHandler
from .pennas import PennasPharmacyHandler
from .ramsay import RamsayHandler
from .revive import ReviveHandler
from .ydc import YdcHandler
from .fullife import FullifeHandler
from .good_price import GoodPriceHandler
from .healthy_pharmacy import HealthyPharmacyHandler
from .healthy_world import HealthyWorldPharmacyHandler
from .wizard import WizardPharmacyHandler
from .superchem import SuperChemHandler

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
    'friendly_care',
    'fullife',
    'good_price',
    'healthy_pharmacy',
    'healthy_world',
    'pennas',
    'wizard',
    'chemist_hub',
    'superchem',
    'direct_chemist'
]